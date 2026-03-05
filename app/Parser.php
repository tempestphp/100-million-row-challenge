<?php

namespace App;

use Exception;

final class Parser
{
	private const string DOMAIN ='https://stitcher.io/blog/';
	private const int PROCESSES = 4;
	private const int READ_BUFFER_SIZE = 8 * 1024 * 1024;
	private const int WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

	public function parse(string $inputPath, string $outputPath): void
	{
	    $fp = fopen($inputPath, 'r');

	    $domainLength = strlen(self::DOMAIN);
	    $pathVisits = [];
	    $pathHash = [];

	    $t = microtime(true);

	    stream_set_read_buffer($fp, self::READ_BUFFER_SIZE);

	    $remainder = '';
	    $chunkNo = 0;
	    $hashPath = [];
	    while (($chunk = fread($fp, self::READ_BUFFER_SIZE)) !== '') {
		    if ($remainder !== '') {
			    $chunk = $remainder . $chunk;
		    }

		    $prevNewLine = -1;
		    $newLine = strpos($chunk, "\n", 0);
		    try {
			    do {
				    $path = substr($chunk, $prevNewLine + 1 + $domainLength, ($newLine - 27) - $prevNewLine - $domainLength);
				    $hash = $hashPath[$path] ?? hash('xxh32', $path, true);

				    $y = ((int) $chunk[$newLine - 23]) * 10 + (int) $chunk[$newLine - 22];
				    $m = ((int) $chunk[$newLine - 20]) * 10 + (int) $chunk[$newLine - 19];
				    $d = ((int) $chunk[$newLine - 17]) * 10 + (int) $chunk[$newLine - 16];

				    $time = ($y << 9) | ($m << 5) | $d;

				    if (!isset($pathHash[$hash])) {
					    $pathHash[$hash] = $path;
					    $hashPath[$path] = $hash;
					    $pathVisits[$hash][$time] = 0;
				    }

				    if (isset($pathVisits[$hash][$time])) {
					    $pathVisits[$hash][$time]++;
				    } else {
					    $pathVisits[$hash][$time] = 1;
				    }

				    $prevNewLine = $newLine;
				    $newLine = strpos($chunk, "\n", $newLine + 1);
			    } while ($newLine !== false);
		    } catch (\ValueError) {
			    $newLine = false;
		    }

		    $remainder = substr($chunk, $prevNewLine + 1);
		    $chunkNo++;
	    }

	    $fp = fopen($outputPath, 'w');
	    stream_set_write_buffer($fp, self::WRITE_BUFFER_SIZE);
	    fwrite($fp, "{\n");
	    foreach ($pathVisits as $hash => $dates) {
		    $line = "    \"\\/blog\\/" . str_replace('/', '\\/', $pathHash[$hash]) . "\": {\n";
		    ksort($dates);

		    foreach ($dates as $time => $count) {
			    $y = $time >> 9;
			    $m = ($time >> 5) & 15;
			    $m = $m < 10 ? ('0' . $m) : $m;
			    $d = $time & 31;
			    $d = $d < 10 ? ('0' . $d) : $d;

			    $line .= "        \"20" . $y . "-" . $m . "-" . $d . "\": " . $count . ",\n";
		    }
		    $line[-2] = "\n";
		    $line[-1] = " ";
		    $line .= "   },\n";

		    fwrite($fp, $line);
	    }

	    fseek($fp, -2, SEEK_CUR);
	    fwrite($fp, "\n}");


    }
}
