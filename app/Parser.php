<?php

namespace App;

final class Parser
{
	public function parse(string $inputPath, string $outputPath): void
	{
		$posts = [];

		$handle = fopen($inputPath, "r");
		while (false !== $line = fgets($handle))
		{
			//https://stitcher.io/blog/shorthand-comparisons-in-php,2022-09-10T13:55:25+00:00
			$path = substr($line, 19, -27);
			$date = substr($line, -26, 10);

			if (!isset($posts[$path][$date]))
				$posts[$path][$date] = 0;
			$posts[$path][$date]++;
		}
		foreach ($posts as &$dates)
			ksort($dates);

		fclose($handle);
		file_put_contents($outputPath, json_encode($posts, JSON_PRETTY_PRINT));
	}
}
