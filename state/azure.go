package state

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/camptocamp/terraboard/config"
	"github.com/hashicorp/terraform/states/statefile"
	log "github.com/sirupsen/logrus"
)

// AZURE is a state provider type, leveraging Azure Blob Storage
type AZURE struct {
	containerURL *azblob.ContainerURL
	ctx          *context.Context
}

// NewAZURE creates a new AZURE object
func NewAZURE(c *config.Config) (AZURE, error) {

	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := c.AZURE.Account, c.AZURE.Key
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatalf("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, c.AZURE.Container))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	container := azblob.NewContainerURL(*URL, p)

	ctx := context.Background()

	return AZURE{
		containerURL: &container,
		ctx:          &ctx,
	}, nil
}

// GetLocks returns a map of locks by State path
func (a *AZURE) GetLocks() (locks map[string]LockInfo, err error) {
	log.Infof("Get State Locks")
	locks = make(map[string]LockInfo)
	return
}

// GetStates returns a states files
func (a *AZURE) GetStates() (states []string, err error) {
	log.Infof("Get State States")
	var keys []string

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := a.containerURL.ListBlobsFlatSegment(*a.ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return keys, err
		}

		// ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			log.Infof("Blob name: " + blobInfo.Name + "\n")
			keys = append(keys, blobInfo.Name)
		}
	}
	states = keys
	return states, nil
}

// GetVersions returns default version because of no versioning in azure
func (a *AZURE) GetVersions(state string) (versions []Version, err error) {
	log.Infof("Get State Versions")
	versions = []Version{}
	versions = append(versions, Version{
		ID:           "default",
		LastModified: time.Now(),
	})
	return
}

// GetState retrieves a single State from the azure blob
func (a *AZURE) GetState(st, versionID string) (sf *statefile.File, err error) {
	log.Infof("Get State %s with version %s", st, versionID)
	blobURL := a.containerURL.NewBlockBlobURL(st)

	downloadResponse, err := blobURL.Download(*a.ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)

	// NOTE: automatically retries are performed if the connection fails
	state := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// Parse the statefile
	sf, err = statefile.Read(state)
	if sf == nil {
		return nil, fmt.Errorf("Unable to parse the statefile for workspace %s version %s", st, versionID)
	}

	return
}
