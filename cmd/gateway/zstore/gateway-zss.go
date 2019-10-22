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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/minio/mc/pkg/console"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
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
  OSS server endpoint. Default ENDPOINT is https://oss.aliyuncs.com

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of OSS storage.
     MINIO_SECRET_KEY: Password or secret key of OSS storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to MinIO host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).

EXAMPLES:
  1. Start minio gateway server for Aliyun OSS backend.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}}

  2. Start minio gateway server for Aliyun OSS backend on custom endpoint.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}Q3AM3UQ867SPQQA43P2F
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
     {{.Prompt}} {{.HelpName}} https://oss.example.com

  3. Start minio gateway server for Aliyun OSS backend with edge caching enabled.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*;*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXPIRY{{.AssignmentOperator}}40
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_MAXUSE{{.AssignmentOperator}}80
     {{.Prompt}} {{.HelpName}}
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

// NewGatewayLayer implements Gateway interface and returns OSS ObjectLayer.
func (g *ZSS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	//var err error

	// Regions and endpoints
	// https://www.alibabacloud.com/help/doc-detail/31837.htm
	if g.host == "" {
		g.host = "https://oss.aliyuncs.com"
	}

	// Initialize oss client object.
	//client, err := oss.New(g.host, creds.AccessKey, creds.SecretKey)
	//if err != nil {
	//return nil, err
	//}

	return &zssObjects{
		//Client: client,
	}, nil
}

// Production - oss is production ready.
func (g *ZSS) Production() bool {
	return true
}

// ossToObjectError converts OSS errors to minio object layer errors.
func ossToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	uploadID := ""
	switch len(params) {
	case 3:
		uploadID = params[2]
		fallthrough
	case 2:
		object = params[1]
		fallthrough
	case 1:
		bucket = params[0]
	}

	ossErr, ok := err.(oss.ServiceError)
	if !ok {
		// We don't interpret non OSS errors. As oss errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	switch ossErr.Code {
	case "BucketAlreadyExists":
		err = minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case "BucketNotEmpty":
		err = minio.BucketNotEmpty{Bucket: bucket}
	case "InvalidBucketName":
		err = minio.BucketNameInvalid{Bucket: bucket}
	case "NoSuchBucket":
		err = minio.BucketNotFound{Bucket: bucket}
	case "NoSuchKey":
		if object != "" {
			err = minio.ObjectNotFound{Bucket: bucket, Object: object}
		} else {
			err = minio.BucketNotFound{Bucket: bucket}
		}
	case "InvalidObjectName":
		err = minio.ObjectNameInvalid{Bucket: bucket, Object: object}
	case "AccessDenied":
		err = minio.PrefixAccessDenied{Bucket: bucket, Object: object}
	case "NoSuchUpload":
		err = minio.InvalidUploadID{UploadID: uploadID}
	case "EntityTooSmall":
		err = minio.PartTooSmall{}
	case "SignatureDoesNotMatch":
		err = minio.SignatureDoesNotMatch{}
	case "InvalidPart":
		err = minio.InvalidPart{}
	}

	return err
}

// ossObjects implements gateway for Aliyun Object Storage Service.
type zssObjects struct {
	minio.GatewayUnsupported
	//Client *oss.Client  TODO
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *zssObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to OSS backend.
func (l *zssObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return
}

// MakeBucketWithLocation creates a new container on OSS backend.
func (l *zssObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	console.Printf("zss MakeBucketWithLocation, %s\n", bucket)
	return nil
}

// GetBucketInfo - Get bucket metadata..
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

// DeleteBucket deletes a bucket on OSS.
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
	data := r.Reader
	if err != nil {
		log.Fatalln(err)
	}

	body := &bytes.Buffer{}
	multiPartWriter := multipart.NewWriter(body)

	fileWriter, err := multiPartWriter.CreateFormFile("file", "test4.log")
	if err != nil {
		log.Fatalln(err)
	}
	_, err = io.Copy(fileWriter, data)
	if err != nil {
		log.Fatalln(err)
	}

	multiPartWriter.Close()

	req, err := http.NewRequest("POST", "http://zstoreservice.vip.qa.ebay.com/objects/v1/0:0/9223372036854775807", body)

	req.Header.Set("Content-Type", multiPartWriter.FormDataContentType())
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
	}
	respBody := &bytes.Buffer{}
	_, err = respBody.ReadFrom(resp.Body)

	log.Println(resp.StatusCode)
	log.Println(respBody)

	console.Printf("zss PutObject done")
	return objInfo, nil
}

func (l *zssObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	console.Printf("zss GetObject.\n")
	return nil
}

func (l *zssObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	console.Printf("zss GetObjectInfo begin.\n")
	res, err := http.Get("http://zstoreservice.vip.qa.ebay.com/objects/v1/0:0/00000000-0000-0000-3b5a-543800031000")
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
	return objInfo, nil
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *zssObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	console.Printf("zss GetObjectNInfo begin.\n")

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
	//return nil, nil
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
