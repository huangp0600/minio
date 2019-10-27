/*
 * MinIO Cloud Storage, (C) 2017, 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zss

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio-go/pkg/s3utils"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
)

const (
	zssBackend = "zss"
)

func init() {
	const zssGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  ZSS server endpoint.

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of zstore.
     MINIO_SECRET_KEY: Password or secret key of zstore.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to MinIO host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               "zss",
		Usage:              "Ebay Zoom Storage Service (ZSS)",
		Action:             zssGatewayMain,
		CustomHelpTemplate: zssGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway zss' command line.
func zssGatewayMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, zssBackend, 1)
	}

	// Validate gateway arguments.
	host := ctx.Args().First()
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &ZSS{host})
}

// ZSS implements Gateway.
type ZSS struct {
	host string
}

// Name implements Gateway interface.
func (g *ZSS) Name() string {
	return zssBackend
}

// NewGatewayLayer implements Gateway interface and returns ZSS ObjectLayer.
func (g *ZSS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	//var err error

	// Regions and endpoints
	if g.host == "" {
		g.host = "http://zstoreservice.vip.qa.ebay.com"
	}

	// Initialize zss client object.
	//client, err := oss.New(g.host, creds.AccessKey, creds.SecretKey)
	client := &Client{}

	return &zssObjects{
		Client: client,
	}, nil
}

// Production - zss is production ready.
func (g *ZSS) Production() bool {
	return true
}

type (
	// Client ZSS client
	Client struct {
		//Config     *Config      // client configuration
		//Conn       *Conn        // Send HTTP request
		HTTPClient *http.Client //http.Client to use - if nil will make its own
	}

	// ClientOption client option such as UseCname, Timeout, SecurityToken.
	ClientOption func(*Client)
)

// CreateBucket create bucket
func (client Client) CreateBucket(bucketName string) error {
	console.Printf("zss create bucket done.\n")
	return nil
}

// PutObject creates a new object with the incoming data.
func (client Client) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	return objInfo, nil
}

// DeleteBatch delete objects in batch
func (client Client) DeleteBatch(bucketName string) error {
	console.Printf("zss delete batch\n")
	return nil
}

// zssObjects implements zstore API.
type zssObjects struct {
	minio.GatewayUnsupported
	Client *Client
}

// zssIsValidBucketName verifies whether a bucket name is valid.
func zssIsValidBucketName(bucket string) bool {
	// dot is not allowed in bucket name
	if strings.Contains(bucket, ".") {
		return false
	}
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return false
	}
	return true
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *zssObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to ZSS backend.
func (l *zssObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return si
}

var buckets OwnerBuckets
var objects OwnerObjects

// MakeBucketWithLocation creates a new container on ZSS backend.
func (l *zssObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	console.Printf("zss MakeBucketWithLocation: %s, location: %s\n", bucket, location)
	if !zssIsValidBucketName(bucket) {
		logger.LogIf(ctx, minio.BucketNameInvalid{Bucket: bucket})
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	err := zssCreateBucket(bucket, location)
	if err != nil {
		logger.LogIf(ctx, err)
	}
	console.Printf("Afer make bucket, buckets: %+v\n", buckets)
	return err
}

// GetBucketInfo - Get bucket metadata.
func (l *zssObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	// Azure does not have an equivalent call, hence use
	// ListContainers with prefix
	console.Printf("zss GetBucketInfo.\n")
	return minio.BucketInfo{
		Name: bucket,
	}, nil
}

// ListBuckets - Lists all azure containers, uses Azure equivalent ListContainers.
func (l *zssObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	console.Printf("zss ListBuckets.\n")
	return buckets, nil
}

// DeleteBucket deletes a bucket on ZSS.
func (l *zssObjects) DeleteBucket(ctx context.Context, bucket string) error {
	console.Printf("zss DeleteBucket.\n")
	return nil
}

func (l *zssObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	console.Printf("zss ListObjects.\n")
	return result, nil
}

func (l *zssObjects) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	console.Printf("zss PutObject, %s\n", object)
	console.Printf("user meta, %+v\n", opts.UserDefined)
	//TODO
	//call api
	//assume uuid is "sssssgggg"
	objectID := "sssssgggg"
	userMeta := opts.UserDefined
	err = zssAppendObject(bucket, object, objectID, userMeta)
	if err != nil {
		logger.LogIf(ctx, err)
	}
	console.Printf("Afer PutObject, objects: %+v\n", objects)
	return objInfo, nil
}

func (l *zssObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	console.Printf("zss GetObject begin. bucket: %s, object: %s\n", bucket, key)
	//TODO
	//call api
	//uuid := zssGetObjectID(bucket, key)
	uuid := "00000000-0000-0000-3b5a-543800031000"
	downloadURL := "http://zstoreservice.vip.qa.ebay.com/objects/v1/0:0/" + uuid
	res, err := http.Get(downloadURL)
	if err != nil {
		log.Fatal(err)
	}
	robots, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("text: %s", robots)
	console.Printf("zss GetObjectInfo done.\n")
	return nil
}

func (l *zssObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	console.Printf("zss GetObjectInfo begin. bucket: %s, object: %s\n", bucket, object)
	console.Printf("zss GetObjectInfo done.\n")
	return objInfo, nil
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *zssObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	console.Printf("zss GetObjectNInfo begin. bucket: %s, object: %s\n", bucket, object)

	var objInfo minio.ObjectInfo
	objInfo, err = l.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := l.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	console.Printf("zss GetObjectNInfo done.\n")
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts.CheckCopyPrecondFn, pipeCloser)
}

// DeleteObject - Deletes a blob on azure container, uses Azure
// equivalent DeleteBlob API.
func (l *zssObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	console.Printf("zss DeleteObject.\n")
	return nil
}

func (l *zssObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	console.Printf("zss DeleteObjects.\n")
	errs := make([]error, len(objects))
	return errs, nil
}
