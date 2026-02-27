<?php

namespace App;

use Exception;

final class Parser
{
	private const string DOMAIN ='https://stitcher.io';

    public function parse(string $inputPath, string $outputPath): void
    {
	    $fp = fopen($inputPath, 'r');

	    $domainLength = strlen(self::DOMAIN);
	    $pathVisits = [];

	    while (($line = fgetcsv($fp, escape: '')) !== false) {
		    $path = substr($line[0], $domainLength);
		    $date = substr($line[1], 0, 10);

		    $pathVisits[$path][$date] = ($pathVisits[$path][$date] ?? 0) + 1;
	    }

	    foreach ($pathVisits as $path => $dates) {
		    ksort($pathVisits[$path]);
	    }

	    $fp = fopen($outputPath, 'w');
	    fwrite($fp, json_encode($pathVisits, flags: JSON_PRETTY_PRINT));
    }
}
