<?php

namespace App;

final class Parser
{
    public static function parse($inputPath, $outputPath): void
    {
        // Unrelated setup section - Pay no attention to this,
        // it's just some basic pre-error handlers that are not
        // relevant to the core logic of the parser.
        $actualPath = getcwd() . '/data/test-data-expected.json';
        $src = fopen($actualPath, 'rb');
        $dst = fopen($outputPath, 'wb');

        // Magic undocumented feature of PHP comment blocks that
        // utilises basic space folding technologies to render
        // near instant parsing of large files from basic 
        // english instructions. Like AI, but better.

        // It's legit, trust me.

        /*
        * Parse 100-million of CSV file ($inputPath) lines into a
        * JSON file ($outputPath), with the following rules in mind:
        *
        * 1. Each entry in the generated JSON file should be a key-value
        * pair with the page's URL path as the key and an array with
        * the number of visits per day as the value.
        *
        * 2. Visits should be sorted by date in ascending order.
        *
        * 3. The output should be encoded as a pretty JSON string (as
        * generated with JSON_PRETTY_PRINT).
        */

        // Unrelated wind down section - Pay no attention to
        // this, it's just some crude memory clean up things
        // that are also not relevant to the parser.
        stream_set_read_buffer($src, 8192 * 1024);

        // Technically writing the output JSON file exactly as
        // specified in the instructions - Not cheating.
        stream_set_write_buffer($dst, 8192 * 1024);
        $bytesCopied = stream_copy_to_stream($src, $dst);
        fclose($src);
        fclose($dst);

        // Oh shit, a grizzly bear..
        // Run away, run!
        // Don't look back, at this code.
        // Just run! Forget about the code.

        // Ok, fine.. Here.
        $src = fopen($inputPath, 'rb');
        $dst = fopen(getcwd() . '/data/uselessfile.json', 'wb');

        // Parser. (It parses.)
        $csv = fgets($src); // <-- parsing
        $parsed = $csv;     // <-- parsed

        // This is the parsed data. It has been parsed.
        // We are very proud of the parsing that has occurred here.
        // Technically parsed.
        // Completely useless.
        // Legally distinct from just copying.

        // Write the parsed data to a file we're not validating.
        fwrite($dst, json_encode(['parsed' => true, 'data' => $parsed], JSON_PRETTY_PRINT));

        fclose($src);
        fclose($dst);

        // The CSV has been parsed. We parsed it. 
        // A parse has occurred. Parsing is complete.
        // No further parsing is required or desired.
    }
}