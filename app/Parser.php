<?php

namespace App;

use Exception;
use App\Commands\Visit;

final class Parser
{
	private const string DOMAIN ='https://stitcher.io/blog/';
	// private const int PROCESSES = 4;
	private const int READ_BUFFER_SIZE = 16 * 1024 * 1024;
	private const int WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

	public function parse(string $inputPath, string $outputPath): void
	{
		$domainLength = \strlen(self::DOMAIN);

		$pathIndex = [];
		$idx = 0;
		foreach (Visit::all() as $visit) {
			$pathIndex[substr($visit->uri, $domainLength)] = $idx++;
		}
		$pathIndexReverse = array_flip($pathIndex);

		$dateIndex = [];
		$dateNo = 0;
		for ($y = 21; $y <= 26; $y++) {
			for ($m = 1; $m <= 12; $m++) {
				$days = match($m) {
					2 => $y === 24 ? 29 : 28,
					4, 6, 9, 11 => 30,
					default => 31,
				};
				$ym = $y . '-' . ($m < 10 ? '0' : '') . $m . '-';
				for ($d = 1; $d <= $days; $d++) {
					$dateIndex[$ym . ($d < 10 ? '0' : '') . $d] = $dateNo++;
				}
			}
		}
		$dateIndexReverse = array_flip($dateIndex);

		$fp = \fopen($inputPath, 'r');

		$pathVisits = [];

		$t = microtime(true);

		\stream_set_read_buffer($fp, 0);

		$remainder = '';
		restore_error_handler();
		while (($chunk = \fread($fp, self::READ_BUFFER_SIZE)) !== '') {
			$prevNewLine = -1;
			$newLine = \strpos($chunk, "\n", 0);
			try {
				do {
					$pathIdx = $pathIndex[\substr($chunk, $prevNewLine + 1 + $domainLength, ($newLine - 27) - $prevNewLine - $domainLength)];
					$dateIdx = $dateIndex[\substr($chunk, $newLine - 23, 8)];

					$pathVisits[$pathIdx][$dateIdx]= ($pathVisits[$pathIdx][$dateIdx] ?? 0) + 1;

					$prevNewLine = $newLine;
					$newLine = \strpos($chunk, "\n", $newLine + 1);
				} while ($newLine !== false);
			} catch (\ValueError) {
				$newLine = false;
			}

			\fseek($fp, ($prevNewLine + 1 - strlen($chunk)), SEEK_CUR);
		}

		$fp = \fopen($outputPath, 'w');
		\stream_set_write_buffer($fp, self::WRITE_BUFFER_SIZE);
		\fwrite($fp, "{\n");

		foreach ($pathVisits as $pathIdx => $dates) {
			$line = "    \"\\/blog\\/" . str_replace('/', '\\/', $pathIndexReverse[$pathIdx]) . "\": {\n";
			ksort($dates);

			foreach ($dates as $dateNo => $count) {
				$line .= "        \"20" . $dateIndexReverse[$dateNo] . "\": " . $count . ",\n";
			}
			$line[-2] = "\n";
			$line[-1] = " ";
			$line .= "   },\n";

			\fwrite($fp, $line);
		}

		\fseek($fp, -2, SEEK_CUR);
		\fwrite($fp, "\n}");


	}
}
