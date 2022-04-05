package hashmap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/rclone/rclone/fs"
)

// loadDirectoryMap creates a directory map from the provided input.
func loadDirectoryMap(fs *Fs, in io.Reader) (*dirMap, error) {
	dMap := newDirMap(fs)
	if in == nil {
		return dMap, nil
	}
	r := bufio.NewReader(in)
	for {
		entry, err := r.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading map file entry: %w", err)
		}
		entry = strings.TrimSuffix(entry, "\n")
		if entry == "" {
			continue
		}
		split := strings.SplitN(entry, " ", 2)
		if len(split) < 2 {
			return nil, fmt.Errorf("malformed map file, refusing to load: invalid entry %q", entry)
		}
		dMap.newDirEntry(split[1])
	}
	return dMap, nil
}

// Files returns a map mapping from the filename to the hashed path.
func (d *dirEntry) Files(ctx context.Context) (map[string]string, error) {
	if d.files != nil {
		return d.files, nil
	}
	d.files = make(map[string]string)
	obj, err := d.fs.base.NewObject(ctx, path.Join(d.Hash, "map"))
	switch {
	case errors.Is(err, fs.ErrorObjectNotFound):
		// Just create a new directory if it is not present.
		return d.files, nil
	case err != nil:
		return nil, err
	}
	in, err := obj.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("error opening map file: %w", err)
	}
	defer in.Close()
	r := bufio.NewReader(in)
	for {
		entry, err := r.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading map file entry: %w", err)
		}
		entry = strings.TrimSuffix(entry, "\n")
		if entry == "" {
			continue
		}
		split := strings.SplitN(entry, " ", 2)
		if len(split) < 2 {
			return nil, fmt.Errorf("malformed map file, refusing to load: invalid entry %q", entry)
		}
		d.files[split[1]] = split[0]
	}
	return d.files, nil
}

func (d *dirEntry) write(ctx context.Context) error {
	files, err := d.Files(ctx)
	if err != nil {
		return fmt.Errorf("error fetching original list of files: %w", err)
	}
	// Sort the paths to make the file deterministic.
	fileNames := make([]string, 0, len(files))
	for f := range files {
		fileNames = append(fileNames, f)
	}
	sort.Strings(fileNames)
	// Write.
	pr, pw := io.Pipe()
	objInfo := fakeObjInfo{
		remote: path.Join(d.Hash, "map"),
		fs:     d.fs,
		size:   -1,
	}
	go func() {
		for _, fileName := range fileNames {
			hash := files[fileName]
			pw.Write([]byte(hash + " " + fileName + "\n"))
		}
		pw.Close()
	}()
	_, err = d.fs.base.Put(ctx, pr, objInfo)
	return err
}

// dirEntry is a node in the tree of directories.
type dirEntry struct {
	// Path is the path of the node from root.
	Path string
	// Hash is the associated hash with the node.
	Hash string
	// Parent is the parent directory of this directory. It is nil if the
	// dirEntry represents the root directory.
	Parent *dirEntry
	// Children is a list of child directories relative to the directory.
	Children []*dirEntry

	// Files is a list of files mapped from their exposed path to their base
	// path.
	// TODO: Replace with a higher performance map.
	files map[string]string

	// fs is the implementation of hashmap that the directory entry belongs to.
	fs *Fs
}

// dirMap is a map containing directory entries of a filesystem.
// TODO: Replace with a higher performance map.
type dirMap struct {
	// fs is the implementation of the hashmap.
	fs *Fs
	// Hash contains a lookup from the hash of the directory to the actual
	// directory.
	Hash map[string]*dirEntry
	// Path contains a lookup from the path of the directory to the actual
	// directory.
	Path map[string]*dirEntry
}

// newDirMap creates an empty directory map.
func newDirMap(f *Fs) *dirMap {
	dMap := &dirMap{
		fs:   f,
		Hash: make(map[string]*dirEntry, 100000),
		Path: make(map[string]*dirEntry, 100000),
	}
	// Create the root directory in the map.
	dMap.newDirEntry("")
	return dMap
}

// newDirEntry creates an entry of the directory inside the map. It creates
// parent directory automatically if they do not exist.
func (d dirMap) newDirEntry(overlayPath string) {
	var parent *dirEntry
	if overlayPath != "" {
		var ok bool
		parentPath, _ := path.Split(overlayPath)
		parentPath = strings.TrimSuffix(parentPath, "/")
		parent, ok = d.Path[parentPath]
		if !ok {
			// Create the parent directory if it does not exist.
			d.newDirEntry(parentPath)
			parent = d.Path[parentPath]
		}
	}
	hashed := d.fs.hasher(overlayPath)
	entry := &dirEntry{
		Path:     overlayPath,
		Hash:     hashed,
		Parent:   parent,
		Children: make([]*dirEntry, 0),
		fs:       d.fs,
	}
	d.Hash[hashed] = entry
	d.Path[overlayPath] = entry
	if parent != nil {
		parent.Children = append(parent.Children, entry)
	}
}

func (d dirMap) removeEntry(path string) {
	entry, ok := d.Path[path]
	if !ok {
		return
	}
	if entry.Parent == nil {
		panic("cannot remove root directory")
	}
	var idx int
	for k, v := range entry.Parent.Children {
		if v == entry {
			idx = k
			break
		}
	}
	entry.Parent.Children = append(entry.Parent.Children[:idx], entry.Parent.Children[idx+1:]...)
	delete(d.Path, path)
	delete(d.Hash, entry.Hash)
}

func (d dirMap) write(ctx context.Context) error {
	// Sort the paths to make the file deterministic.
	path := make([]string, 0, len(d.Path))
	for p := range d.Path {
		path = append(path, p)
	}
	sort.Strings(path)
	// Write.
	pr, pw := io.Pipe()
	objInfo := fakeObjInfo{
		remote: "map",
		fs:     d.fs,
		size:   -1,
	}
	go func() {
		for _, p := range path {
			entry := d.Path[p]
			pw.Write([]byte(entry.Hash + " " + p + "\n"))
		}
		pw.Close()
	}()
	_, err := d.fs.base.Put(ctx, pr, objInfo)
	return err
}
