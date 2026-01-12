// Copyright 2020 Jonas Dahlbæk

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// 	http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lib

import (
	"context"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/semaphore"
)

type Args struct {
	Concurrency uint
	Limit       uint64
	BufferLimit int64
	Force       bool

	Cpuprofile string
	Memprofile string

	List  bool
	Keys  bool
	Debug bool

	AwsProfile string

	UrlArg []string
}

func Run(args Args) {
	if args.Cpuprofile != "" {
		f, err := os.Create(args.Cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	var configOpts []func(*config.LoadOptions) error
	if args.AwsProfile != "" {
		configOpts = append(configOpts, config.WithSharedConfigProfile(args.AwsProfile))
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), configOpts...)
	if err != nil {
		log.Fatal(err)
	}

	s3Client := s3.NewFromConfig(cfg)

	bytesDownloaded, pooled, streamed := produce(args, s3Client)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(streamed <-chan Object) {
		defer wg.Done()
		for obj := range streamed {
			consumeSequential(obj, s3Client, args.Keys)
		}
	}(streamed)

	sem := semaphore.NewWeighted(args.BufferLimit)
	for i := uint(0); i < args.Concurrency; i++ {
		wg.Add(1)
		go func(pooled <-chan Object) {
			defer wg.Done()
			for obj := range pooled {
				sem.Acquire(context.TODO(), obj.Size)
				consume(obj, s3Client, args.Keys)
				sem.Release(obj.Size)
			}
		}(pooled)
	}
	wg.Wait()

	if *bytesDownloaded > args.Limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", args.Limit, bytesDownloaded)
		os.Exit(1)
	}

	if args.Memprofile != "" {
		f, err := os.Create(args.Memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
