package drive

import (
	"fmt"
	"time"

	. "github.com/claudetech/loggo/default"
	gdrive "google.golang.org/api/drive/v3"
)

// APIObject is a Google Drive file object
type APIObject struct {
	ObjectID     string
	Name         string
	IsDir        bool
	Size         uint64
	LastModified time.Time
	Parents      []string
	CanTrash     bool
	MD5Checksum  string
	RevisionID   string
}

// NewAPIObject creates a new APIObject from a Drive SDK v3 File Resource
func NewAPIObject(file *gdrive.File) (*APIObject, error) {
	lastModified, err := time.Parse(time.RFC3339, file.ModifiedTime)
	if nil != err {
		Log.Debugf("%v", err)
		Log.Warningf("Could not parse last modification time %v for object %v (%v)", file.ModifiedTime, file.Id, file.Name)
		lastModified = time.Now()
	}

	return &APIObject{
		ObjectID:     file.Id,
		Name:         file.Name,
		IsDir:        file.MimeType == "application/vnd.google-apps.folder",
		LastModified: lastModified,
		Size:         uint64(file.Size),
		Parents:      file.Parents,
		CanTrash:     file.Capabilities.CanTrash,
		MD5Checksum:  file.Md5Checksum,
		RevisionID:   file.HeadRevisionId,
	}, nil
}

// DownloadURL returns the url to download the current revision of the api object
func (o *APIObject) DownloadURL() string {
	if o.IsDir {
		return ""
	}
	if "" == o.RevisionID {
		// Usage of v2 API here only for consistency, v3 works fine.
		// Maybe v2 with "source=downloadUrl" could be faster (needs testing).
		// return fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%v?alt=media", o.ObjectID)
		return fmt.Sprintf("https://www.googleapis.com/drive/v2/files/%v?alt=media", o.ObjectID)
	}
	// Unfortunately the v3 revisions API currently does not support range requests, so we use v2 API.
	// See: https://issuetracker.google.com/issues/171904226
	// return fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%v/revisions/%v?alt=media", o.ObjectID, o.RevisionID)
	return fmt.Sprintf("https://www.googleapis.com/drive/v2/files/%v?alt=media&revisionId=%v", o.ObjectID, o.RevisionID)
}
