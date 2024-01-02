package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/spf13/cobra"
)

type globalConfig struct {
	cpuprofile     string
	memprofile     string
	memprofilerate int
	verbose        bool
}

func main() {
	var (
		cfg  globalConfig
		cpuf *os.File
	)

	rootCmd := &cobra.Command{
		Use:   "updog",
		Short: `updog is a static index to quickly count elements and optionally group them.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return fmt.Errorf("showing help failed: %v", err)
			}

			return nil
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if cfg.cpuprofile != "" {
				f, err := os.Create(cfg.cpuprofile)
				if err != nil {
					return fmt.Errorf("could not create CPU profile: %w", err)
				}
				cpuf = f
				if err := pprof.StartCPUProfile(f); err != nil {
					return fmt.Errorf("could not start CPU profile: %w", err)
				}
			}

			if cfg.memprofile != "" {
				runtime.MemProfileRate = cfg.memprofilerate
			}

			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if cfg.cpuprofile != "" {
				pprof.StopCPUProfile()
				cpuf.Close()
			}

			if cfg.memprofile != "" {
				f, err := os.Create(cfg.memprofile)
				if err != nil {
					return fmt.Errorf("could not create memory profile: %w", err)
				}
				defer f.Close()
				runtime.GC()
				if err := pprof.WriteHeapProfile(f); err != nil {
					return fmt.Errorf("could not write memory profile: %w", err)
				}
			}

			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&cfg.cpuprofile, "cpuprofile", "", "if non-empty, write CPU profile to this file")
	rootCmd.PersistentFlags().StringVar(&cfg.memprofile, "memprofile", "", "if non-empty, write memory profile to this file")
	rootCmd.PersistentFlags().IntVar(&cfg.memprofilerate, "memprofilerate", runtime.MemProfileRate, "memory profile rate")
	rootCmd.PersistentFlags().BoolVarP(&cfg.verbose, "verbose", "v", false, "if enabled, make output more verbose")

	var serverCfg serverConfig

	serverCmd := &cobra.Command{
		Use:   "server",
		Short: `Start a new updog gRPC server to make an updog index available remotely.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return serverCmd(&serverCfg)
		},
	}

	serverCmd.PersistentFlags().StringVarP(&serverCfg.addr, "listen", "l", ":8734", "listen address for gRPC server")
	serverCmd.PersistentFlags().StringVarP(&serverCfg.debugAddr, "debug-listen", "d", ":8735", "listen address for debug HTTP server exposing prometheus metrics and Go pprof interface")
	serverCmd.PersistentFlags().StringVarP(&serverCfg.indexFile, "index-file", "f", "out.updog", "index file to load")
	serverCmd.PersistentFlags().BoolVarP(&serverCfg.enableCache, "enable-cache", "c", true, "enable query cache")
	serverCmd.PersistentFlags().Uint64VarP(&serverCfg.maxCacheSize, "max-cache-size", "s", 50*1024*1024, "maximum query cache size")
	serverCmd.PersistentFlags().BoolVarP(&serverCfg.enablePreloadedData, "enable-preloaded-data", "p", false, "enable preloaded data")

	var clientCfg clientConfig

	clientCmd := &cobra.Command{
		Use:   "client",
		Short: `Remotely query an updog gRPC server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return clientCmd(&clientCfg, args)
		},
	}

	clientCmd.PersistentFlags().StringVarP(&clientCfg.addr, "connect", "c", "localhost:8734", "gRPC server address to connect to")

	var createCfg createConfig

	createCmd := &cobra.Command{
		Use:   "create",
		Short: `Create an updog index file from a CSV file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("no input file provided")
			}
			if len(args) > 1 {
				return fmt.Errorf("more than one input file provided")
			}

			createCfg.inputFile = args[0]

			return createCmd(&cfg, &createCfg)
		},
	}

	createCmd.PersistentFlags().StringVarP(&createCfg.outputFile, "output", "o", "out.updog", "output index file")
	createCmd.PersistentFlags().BoolVarP(&createCfg.big, "big", "b", false, "enable big mode that allows you to create files larger than the available memory, but creation will be slower")

	var schemaCfg schemaConfig

	schemaCmd := &cobra.Command{
		Use:   "schema",
		Short: `Show schema of updog index file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return schemaCmd(&schemaCfg)
		},
	}

	schemaCmd.PersistentFlags().StringVarP(&schemaCfg.indexFile, "index-file", "f", "out.updog", "index file to introspect")
	schemaCmd.PersistentFlags().BoolVar(&schemaCfg.full, "full", false, "show all available values")

	rootCmd.AddCommand(serverCmd, clientCmd, createCmd, schemaCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
