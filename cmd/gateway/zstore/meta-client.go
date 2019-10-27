package zss

import (
	"fmt"
	"time"

	minio "github.com/minio/minio/cmd"
)

// BucketInfo stores bucket info
type BucketInfo struct {
	OwnerID     string
	Name        string
	Location    string
	CreatedTime time.Time
}

// ObjectInfo stores bucket info
type ObjectInfo struct {
	OwnerID        string
	Bucket         string
	Name           string
	ObjectID       string
	ModTime        time.Time
	LastAccessTime time.Time
	ExpireTime     time.Time
	StorageClass   string
	Size           int64
	AccessTimes    int64
}

// OwnerBuckets store buckets
type OwnerBuckets []BucketInfo

// OwnerObjects store objects
type OwnerObjects []ObjectInfo

// zssCreateBucket oo.
func zssCreateBucket(bucket, location string) error {
	for _, bktInfo := range buckets {
		if bucket == bktInfo.Name {
			return minio.BucketAlreadyExists{Bucket: bucket}
		}
	}
	bkt := BucketInfo{Name: bucket, Location: location, CreatedTime: time.Now()}
	buckets = append(buckets, bkt)
	return nil
}

// isBucketExist oo.
func isBucketExist(bucket string) {
	fmt.Println("test")
}

// GetBucketInfo oo.
func GetBucketInfo(bucket string) {
	fmt.Println("test")
}

// DeleteBucket oo.
func DeleteBucket(bucket string) {
	fmt.Println("test")
}

// GetBuckets oo.
func GetBuckets(bucket string) {
	fmt.Println("test")
}

// zssAppendObject oo.
func zssAppendObject(bucket, object, objectID string, meta map[string]string) error {
	var objectInfo ObjectInfo
	objectInfo.Bucket = bucket
	objectInfo.Name = object
	objectInfo.ObjectID = objectID
	for key, value := range meta {
		if key == "X-Amz-Meta-Storageclass" {
			objectInfo.StorageClass = value
		}
	}
	objects = append(objects, objectInfo)
	return nil
}

//zssGetObjectInfo oo.
func zssGetObjectID(bucket, object string) string {
	for _, objectInfo := range objects {
		if objectInfo.Bucket == bucket && objectInfo.Name == object {
			return objectInfo.ObjectID
		}
	}
	return ""
}

// DeleteObject oo.
func DeleteObject(bucket string) {
	fmt.Println("test")
}

// DeleteObjects oo.
func DeleteObjects(bucket string) {
	fmt.Println("test")
}

// ListObjects oo.
func ListObjects(bucket string) {
	fmt.Println("test")
}

// GetObject oo.
func GetObject(bucket string) {
	fmt.Println("test")
}

// GetObjectInfo oo.
func GetObjectInfo(bucket string) {
	fmt.Println("test")
}
