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

	var serverConfig struct {
		addr      string
		debugAddr string
	}

	serverCmd := &cobra.Command{
		Use:   "server",
		Short: `Start a new updog gRPC server to make an updog index available remotely.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("Address: %s\nDebug address: %s\n", serverConfig.addr, serverConfig.debugAddr)
			// TODO: implement
			return nil
		},
	}

	serverCmd.PersistentFlags().StringVarP(&serverConfig.addr, "listen", "l", ":8734", "listen address for gRPC server")
	serverCmd.PersistentFlags().StringVarP(&serverConfig.debugAddr, "debug-listen", "d", ":8735", "listen address for debug HTTP server exposing prometheus metrics and Go pprof interface")

	var clientConfig struct {
		addr string
	}

	clientCmd := &cobra.Command{
		Use:   "client",
		Short: `Remotely query an updog gRPC server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("Server address: %s\n", clientConfig.addr)
			// TODO: implement
			return nil
		},
	}

	clientCmd.PersistentFlags().StringVarP(&clientConfig.addr, "connect", "c", "localhost:8734", "gRPC server address to connect to")

	var createConfig struct {
		outputFile string
	}

	createCmd := &cobra.Command{
		Use:   "create",
		Short: `Create an updog index file from a CSV file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("Output file: %s\n", createConfig.outputFile)
			// TODO: implement
			return nil
		},
	}

	createCmd.PersistentFlags().StringVarP(&createConfig.outputFile, "output", "o", "out.updog", "output index file")

	rootCmd.AddCommand(serverCmd, clientCmd, createCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
