<?php

namespace App;

final class Parser
{
	public function parse(string $inputPath, string $outputPath): void
	{
		gc_disable();
		$posts = [];
		$chunk_size = 16 * 1024;

		$handle = fopen($inputPath, "r");
		stream_set_read_buffer($handle, 0);
		$last_line = '';
		while (!feof($handle))
		{
			$chunk = fread($handle, $chunk_size);
			$lines = explode("\n", $chunk);
			$count = count($lines);
			for ($i = 0; $i < $count; $i++)
			{
				if ($i === $count - 1)
				{
					$last_line = $lines[$i];
				}
				else
				{
					if ($i === 0)
						$line = $last_line . $lines[$i];
					else
						$line = $lines[$i];

					$path = substr($line, 19, -26);
					$date = substr($line, -25, 10);
					$posts[$path][] = $date;
				}
			}
		}
		fclose($handle);
		foreach ($posts as &$dates)
		{
			$dates = array_count_values($dates);
			ksort($dates);
		}

		file_put_contents($outputPath, json_encode($posts, JSON_PRETTY_PRINT));
	}
}
