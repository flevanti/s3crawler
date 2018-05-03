package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"fmt"
	"os"
	"github.com/joho/godotenv"
	"log"
)

var bucket string
var region string
var marker = ""
var recordsCount = 0

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	region = os.Getenv("AWS_BUCKET_REGION")
	bucket = os.Getenv("AWS_BUCKET_NAME")

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	// Create S3 service client
	svc := s3.New(sess)
	/*
		result, err := svc.ListBuckets(nil)
		if err != nil {
			exitErrorf("Unable to list buckets, %v", err)
		}

		fmt.Println("Buckets:")

		for _, b := range result.Buckets {
			fmt.Printf("* %s created on %s\n ",
				aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
		}

	*/

	for {
		resp, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket),
			MaxKeys: aws.Int64(1000),
			Delimiter: aws.String("/"),
			Prefix: aws.String(""),
			Marker: &marker})
		if err != nil {
			exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
		}
		/*
				for _, item := range resp.Contents {
					fmt.Println("Name:         ", *item.Key)
					fmt.Println("Last modified:", *item.LastModified)
					fmt.Println("Size:         ", *item.Size)
					fmt.Println("Storage class:", *item.StorageClass)
					fmt.Println("ETAG         :", *item.ETag)
					fmt.Println("")
					marker = *item.Key
				} //end foreach record
		*/
		recordsCount += len(resp.Contents)
		fmt.Println(resp.CommonPrefixes)

		if *resp.IsTruncated != true {
			break
		}

	} //end infinite loop

	fmt.Println("Records extracted: ", recordsCount)

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
