// Package hashmap provides wrappers for Fs and Object which hash and map file names.
package hashmap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "hashmap",
		Description: "Transparently hash file names",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "remote",
			Required: true,
			Help: `Remote to hash/unhash.

Normally should contain a ':' and a path, e.g. "myremote:path/to/dir",
"myremote:bucket" or maybe "myremote:" (not recommended).`,
		}, {
			Name:     "hash_type",
			Advanced: false,
			Default:  "md5",
			Help: `Choose how hasher hashes filenames.

All modes but "none" require metadata.`,
			Examples: []fs.OptionExample{{
				Value: "md5",
			}, {
				Value: "md5",
				Help:  `MD5 for hashes.`,
			}, {
				Value: "sha1",
				Help:  `SHA1 for hashes .`,
			}, {
				Value: "sha256",
				Help:  `SHA256 for hashes.`,
			}},
		}},
	})
}

// Fs is the implementation of the backend.
type Fs struct {
	// base is the FS that this Fs wraps around.
	base fs.Fs
	// opt is the Options used to configure the Fs.
	opt Options
	// hasher is the function mapping the name of directories and files to the
	// hashed version.
	hasher func(string) string
	// dirMap is the map containing information on the directory structure of
	// the FS.
	dirMap *dirMap

	// name is the name of the Fs as passed into NewFs.
	name string
	// root is the path to the root of the Fs as passed into NewFs.
	root string
	// feat is the list of features supported by the FS computed in NewFs.
	feat *fs.Features
	// wrapper is used to implement fs.Wrapper.
	wrapper fs.Fs
}

// Options is the configuration for the backend.
type Options struct {
	Remote   string `config:"remote"`
	HashType string `config:"hash_type"`
}

// NewFs constructs a hashmap.Fs with the provided configuration.
func NewFs(ctx context.Context, name, rpath string, m configmap.Mapper) (fs.Fs, error) {
	// Parse configuration into Options.
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	// Construct the remote to wrap around.
	if strings.HasPrefix(opt.Remote, name+":") {
		return nil, errors.New("can't point remote at itself - check the value of the remote setting")
	}
	baseFs, err := cache.Get(ctx, opt.Remote)
	if err != fs.ErrorIsFile && err != nil {
		return nil, fmt.Errorf("failed to make remote %q to wrap: %w", opt.Remote, err)
	}

	// Construct the actual FS.
	f := &Fs{
		base: baseFs,
		opt:  *opt,
		name: name,
		root: rpath,
	}
	switch opt.HashType {
	case "none":
		f.hasher = hashNone
	case "md5":
		f.hasher = hashMD5
	case "sha1":
		f.hasher = hashSHA1
	case "sha256":
		f.hasher = hashSHA256
	default:
		return nil, fmt.Errorf("unknown hash type %q", opt.HashType)
	}

	feat := &fs.Features{
		CaseInsensitive:         false,
		DuplicateFiles:          true,
		ReadMimeType:            true,
		WriteMimeType:           true,
		BucketBased:             true,
		CanHaveEmptyDirectories: true,
		ServerSideAcrossConfigs: true,
	}
	feat.Fill(ctx, f).Mask(ctx, f.base).WrapsFs(f, f.base)
	// We always create a map file so the base FS doesn't need to actually
	// support empty directories.
	feat.CanHaveEmptyDirectories = true
	f.feat = feat

	// Keep baseFs alive until this FS is garbage-collected.
	cache.PinUntilFinalized(f.base, f)

	// Load the directory map.
	var r io.ReadCloser
	obj, err := f.base.NewObject(ctx, "map")
	if err == nil {
		r, err = obj.Open(ctx)
		switch {
		case errors.Is(err, fs.ErrorObjectNotFound):
			// Just create an empty map.
			r = nil
		case err != nil:
			return nil, err
		default:
			defer r.Close()
		}
	}
	f.dirMap, err = loadDirectoryMap(f, r)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Name returns the name of the Fs as passed into NewFs.
func (f *Fs) Name() string {
	return f.name
}

// Root returns the name of the Fs as passed into NewFs.
func (f *Fs) Root() string {
	return f.root
}

// String returns a string description of the FS.
func (f *Fs) String() string {
	return fmt.Sprintf("Hashmap (%s) '%s:%s'", f.opt.HashType, f.name, f.root)
}

// Precision returns the mod time precision of the FS.
func (f *Fs) Precision() time.Duration {
	// We just pass on the mod time. Therefore, it's reliant on the base Fs.
	return f.base.Precision()
}

// Hashes returns the set of file hashes supported by the FS.
func (f *Fs) Hashes() hash.Set {
	// We just pass on the hash. Therefore, it's reliant on the base Fs.
	return f.base.Hashes()
}

// Features returns the list of features supported by the FS.
func (f *Fs) Features() *fs.Features {
	return f.feat
}

// About returns quota information from the base Fs.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	do := f.base.Features().About
	if do == nil {
		return nil, errors.New("About not supported")
	}
	return do(ctx)
}

// UnWrap returns the Fs that this Fs is wrapping.
func (f *Fs) UnWrap() fs.Fs {
	return f.base
}

// Shutdown triggers shutdown on the base FS.
func (f *Fs) Shutdown(ctx context.Context) error {
	do := f.base.Features().Shutdown
	if do == nil {
		return nil
	}
	return do(ctx)
}

// CleanUp removes trash in the Fs. It is implemented if the Fs has a way of
// emptying the trash or otherwise cleaning up old versions of files.
//
// This is implemented by delegating to the base FS.
func (f *Fs) CleanUp(ctx context.Context) error {
	do := f.base.Features().CleanUp
	if do == nil {
		return errors.New("can't CleanUp")
	}
	return do(ctx)
}

// WrapFs returns the Fs that is currently wrapping this Fs.
func (f *Fs) WrapFs() fs.Fs {
	return f.wrapper
}

// SetWrapper sets the Fs that is currently wrapping this Fs.
func (f *Fs) SetWrapper(wrapper fs.Fs) {
	f.wrapper = wrapper
}

// Check that interfaces are implemented.
var (
	_ fs.Abouter        = (*Fs)(nil)
	_ fs.ChangeNotifier = (*Fs)(nil)
	_ fs.CleanUpper     = (*Fs)(nil)
	_ fs.Copier         = (*Fs)(nil)
	_ fs.Purger         = (*Fs)(nil)
	_ fs.PutStreamer    = (*Fs)(nil)
	_ fs.PutUncheckeder = (*Fs)(nil)
	_ fs.Shutdowner     = (*Fs)(nil)
	_ fs.UnWrapper      = (*Fs)(nil)
	_ fs.Wrapper        = (*Fs)(nil)
)
