package main

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var bucket = "test-restore-test-bucket"

var (
	maxThreads         = 5
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSRegion          string
	bucketName         string
	prefixesFile       string
	prefixes           []string
	dryRun             = true
	totalRestored      int
)

var s3svc *s3.S3 = s3.New(getSession())

func getSession() *session.Session {

	if AWSAccessKeyID == "" {
		AWSAccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	if AWSSecretAccessKey == "" {
		AWSSecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	AWSRegion = "us-east-1"
	awsConf := aws.Config{Credentials: credentials.NewStaticCredentials(AWSAccessKeyID, AWSSecretAccessKey, ""), Region: &AWSRegion}
	sess, err := session.NewSessionWithOptions(session.Options{Config: awsConf})
	if err != nil {
		fmt.Println("failed to create AWS session,", err)
		os.Exit(1)
	}

	return sess
}

func restoreStarter() {
	t0 := time.Now()

	var wg sync.WaitGroup

	prefixesCount := len(prefixes)
	fmt.Println("Total Prefixes:", prefixesCount)
	getRestoreQueue := make(chan int, maxThreads)

	for i := 0; i < prefixesCount; i++ {
		getRestoreQueue <- 1
		wg.Add(1)
		go restoreByPrefix(prefixes[i], &wg, getRestoreQueue, i)
		fmt.Println("--- Started thread with prefix:", prefixes[i])
	}

	wg.Wait()
	fmt.Println("=== All done in: ", time.Now().Sub(t0).Seconds(), "restored objects:", totalRestored)
}

func restoreByPrefix(prefix string, wg *sync.WaitGroup, getRestoreQueue chan int, num int) {
	t0 := time.Now()
	defer wg.Done()

	objects, err := s3svc.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &prefix,
	})

	if err != nil {
		panic(err)
	}

	restoreS3Object(prefix, objects.DeleteMarkers)

	keyMarker := objects.NextKeyMarker
	for keyMarker != nil {
		objects, err := s3svc.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket:    &bucketName,
			Prefix:    &prefix,
			KeyMarker: keyMarker,
		})

		if err != nil {
			panic(err)
		}
		restoreS3Object(prefix, objects.DeleteMarkers)
		keyMarker = objects.NextKeyMarker
	}
	<-getRestoreQueue

	fmt.Println("--- Prefix", prefix, "done in", time.Now().Sub(t0).Seconds())
	return
}

func restoreS3Object(prefix string, markers []*s3.DeleteMarkerEntry) {
	t0 := time.Now()
	for _, marker := range markers {
		if *marker.IsLatest {
			if strings.HasPrefix(*marker.Key, prefix) {
				if dryRun {
					fmt.Printf("*** Would have restored %s\n", *marker.Key)
				} else {
					fmt.Printf("*** Restoring %s\n", *marker.Key)
					s3svc.DeleteObject(&s3.DeleteObjectInput{
						Bucket:    &bucketName,
						Key:       marker.Key,
						VersionId: marker.VersionId,
					})
					fmt.Println("***", *marker.Key, "restored in", time.Now().Sub(t0).Seconds())
					totalRestored++
				}
			}
		}
	}
}

func showObjects() {
	parsePrefixesFile()
	dryRun = true
	restoreStarter()
}

func restoreObjects() {
	parsePrefixesFile()
	dryRun = false
	restoreStarter()
}

func parsePrefixesFile() {
	file, err := os.Open(prefixesFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		p := strings.Replace(strings.Split(scanner.Text(), ",")[0], "\"", "", -1)

		if p != "embed_code" && p != "" && len(p) > 0 {
			prefixes = append(prefixes, p)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	fmt.Println()
}

func main() {

	programName := filepath.Base(os.Args[0])

	var cmdShowObjects = &cobra.Command{
		Use:   "show",
		Short: "Shows list of all deleted objects in bucket",
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			showObjects()
		},
	}

	var cmdRestoreObjects = &cobra.Command{
		Use:   "restore",
		Short: "Restore all deleted objects in bucket",
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			restoreObjects()
		},
	}

	var rootCmd = &cobra.Command{Use: programName}
	rootCmd.PersistentFlags().StringVarP(&AWSAccessKeyID, "aws_access_key_id", "i", "", "AWS access key ID (default from env AWS_ACCESS_KEY_ID)")
	rootCmd.PersistentFlags().StringVarP(&AWSSecretAccessKey, "aws_secret_access_key", "k", "", "AWS secret access key (default from env AWS_SECRET_ACCESS_KEY)")
	rootCmd.PersistentFlags().StringVarP(&AWSRegion, "aws_region", "r", "us-east-1", "AWS region")
	rootCmd.PersistentFlags().IntVarP(&maxThreads, "threads", "t", 5, "Count of threads")

	rootCmd.PersistentFlags().StringVarP(&bucketName, "bucket_name", "b", "", "AWS S3 bucket name")
	rootCmd.MarkPersistentFlagRequired("bucket_name")
	rootCmd.PersistentFlags().StringVarP(&prefixesFile, "prefixes_file_name", "f", "", "Prefixes file name")
	rootCmd.MarkPersistentFlagRequired("prefixes_file_name")

	rootCmd.AddCommand(cmdRestoreObjects)
	rootCmd.AddCommand(cmdShowObjects)
	rootCmd.Execute()

	fmt.Println()
}
