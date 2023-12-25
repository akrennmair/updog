package main

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "updog",
		Short: `updog is a static index to quickly count elements and optionally group them.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return fmt.Errorf("showing help failed: %v", err)
			}

			return nil
		},
	}

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

			return createCmd(&createCfg)
		},
	}

	createCmd.PersistentFlags().StringVarP(&createCfg.outputFile, "output", "o", "out.updog", "output index file")

	rootCmd.AddCommand(serverCmd, clientCmd, createCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
