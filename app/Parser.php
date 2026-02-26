<?php

namespace App;

final class Parser
{
	public function parse(string $inputPath, string $outputPath): void
	{
		$input = file_get_contents($inputPath);
		$lines = explode("\n", $input);
		$posts = [];
		foreach ($lines as $line)
		{
			if (empty($line))
				continue;
			$a = substr($line, 19) . "\n";
			$b = explode(",", $a);

			[$path, $date] = $b;
			$date = substr($date, 0, 10);
			if (!isset($posts[$path][$date]))
				$posts[$path][$date] = 0;
			$posts[$path][$date]++;
		}
		foreach ($posts as &$dates)
			ksort($dates);

		file_put_contents($outputPath, json_encode($posts, JSON_PRETTY_PRINT));
	}
}
