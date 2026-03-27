<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $map = array();

        if ( ( $handle = fopen( $inputPath, "r" ) ) !== false ) {
            while ( $line = fgets( $handle ) ) {
                // No need for slow CSV parsing when the format is known.
                list( $url, $timestamp ) = explode( ',', $line, 2 );

                // Domain is a known length, ignore it.
                $path = substr( $url, 19 );

                // Timestamp is a known format, just grab what we need.
                $date = substr( $timestamp, 0, 10 );

                if ( ! isset( $map[ $path ] ) ) {
                    $map[ $path ] = array(
                        $date => 1
                    );
                } else {
                    $pointer = &$map[ $path ];

                    if ( ! isset( $pointer[ $date ] ) ) {
                        $pointer[ $date ] = 1;
                    } else {
                        ++$pointer[ $date ];
                    }
                }

            }

            fclose( $handle );
        }

        // Sort by date ascending.
        foreach ( $map as $path => $visits ) {
            ksort( $map[ $path ] );
        }

        file_put_contents( $outputPath, json_encode( $map, JSON_PRETTY_PRINT ) );
    }
}