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
	DownloadURL  string
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

	var downloadURL string
	if "" != file.HeadRevisionId {
		// Unfortunately the v3 revisions API currently does not support range requests,
		// so we need to fall back to v2 API to download revisions.
		// downloadURL = fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%v/revisions/%v?alt=media", file.Id, file.HeadRevisionId)
		downloadURL = fmt.Sprintf("https://www.googleapis.com/drive/v2/files/%v?alt=media&revisionId=%v", file.Id, file.HeadRevisionId)
	} else {
		// Usage of v2 API here only for consistency, v3 works fine.
		// Maybe v2 with "source=downloadUrl" could be faster (needs testing).
		// downloadURL = fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%v?alt=media", file.Id)
		downloadURL = fmt.Sprintf("https://www.googleapis.com/drive/v2/files/%v?alt=media", file.Id)
	}

	return &APIObject{
		ObjectID:     file.Id,
		Name:         file.Name,
		IsDir:        file.MimeType == "application/vnd.google-apps.folder",
		LastModified: lastModified,
		DownloadURL:  downloadURL,
		Size:         uint64(file.Size),
		Parents:      file.Parents,
		CanTrash:     file.Capabilities.CanTrash,
		MD5Checksum:  file.Md5Checksum,
		RevisionID:   file.HeadRevisionId,
	}, nil
}
