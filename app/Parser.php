<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
		// Just to start with
        //self::parseDefault($inputPath, $outputPath);

		// Seems to be the fastest option for smaller datasets
		self::parseOptimizedWithSort($inputPath, $outputPath);

		// Seems to be the fastest option for 10 mil rows (but slightly higher memory usage)
		// it is much slower for the validate case than parseOptimizedWithSort
		//self::parseOptimizedCountingSort($inputPath, $outputPath);
    }

	private static function parseDefault(string $inputPath, string $outputPath): void
	{
		$visits = [];

		$inputHandle = fopen($inputPath,'r');
		while (false !== ($row = fgetcsv($inputHandle, escape: ''))) {
			$page = $row[0];
			$path = parse_url($page, PHP_URL_PATH);

			$date = $row[1];
			$day = date('Y-m-d', strtotime($date));

			if (false == isset($visits[$path])) {
				$visits[$path] = [];
			}

			if (false == isset($visits[$path][$day])) {
				$visits[$path][$day] = 1;
			} else {
				$visits[$path][$day] += 1;
			}
		}
		fclose($inputHandle);

		foreach (array_keys($visits) as $path) {
			ksort($visits[$path]);
		}

		$outputHandle = fopen($outputPath,'w');
		fwrite($outputHandle, json_encode($visits, JSON_PRETTY_PRINT));
		fclose($outputHandle);
	}

	private static function parseOptimizedWithSort(string $inputPath, string $outputPath): void
	{
		$visits = [];

		// could hardcode it
		$pathStartPos = strlen('https://stitcher.io');
		$dayLength = strlen('2025-01-01');

		$inputHandle = fopen($inputPath,'r');
		while (false !== ($row = fgetcsv($inputHandle, escape: ''))) {
			$page = $row[0];
			$path = substr($page, $pathStartPos);

			$date = $row[1];
			$day = substr($date, 0, $dayLength);

			if (false == isset($visits[$path])) {
				$visits[$path] = [];
			}

			if (false == isset($visits[$path][$day])) {
				$visits[$path][$day] = 1;
			} else {
				$visits[$path][$day] += 1;
			}
		}
		fclose($inputHandle);

		foreach (array_keys($visits) as $path) {
			ksort($visits[$path]);
		}

		$outputHandle = fopen($outputPath,'w');
		fwrite($outputHandle, json_encode($visits, JSON_PRETTY_PRINT));
		fclose($outputHandle);
	}

	private static function parseOptimizedCountingSort(string $inputPath, string $outputPath): void
	{
		$visits = [];

		// could hardcode it
		$pathStartPos = strlen('https://stitcher.io');

		$inputHandle = fopen($inputPath,'r');
		while (false !== ($row = fgetcsv($inputHandle, escape: ''))) {
			$page = $row[0];
			$path = substr($page, $pathStartPos);

			$date = $row[1];
			// Convert day into number representation to avoid further sort
			$day = self::convertDayToNumber($date);

			if (false == isset($visits[$path])) {
				$visits[$path] = [];
			}

			if (false == isset($visits[$path][$day])) {
				$visits[$path][$day] = 1;
			} else {
				$visits[$path][$day] += 1;
			}
		}
		fclose($inputHandle);

		foreach ($visits as $path => $pathVisits) {
			$sortedPathVisits = self::sortPathDailyVisits($pathVisits);
			$pathVisitsWithConvertedDay = [];
			foreach ($sortedPathVisits as $day => $dayVisitsCount) {
				$pathVisitsWithConvertedDay[self::convertNumberToDay($day)] = $dayVisitsCount;
			}

			$visits[$path] = $pathVisitsWithConvertedDay;
		}

		$outputHandle = fopen($outputPath,'w');
		fwrite($outputHandle, json_encode($visits, JSON_PRETTY_PRINT));
		fclose($outputHandle);
	}

	private static function convertDayToNumber(string $date): int
	{
		$year = substr($date, 0, 4);
		$month = substr($date, 5, 2);
		$day = substr($date, 8, 2);

		return (int) ($year . $month . $day);
	}

	private static function convertNumberToDay(int $n): string
	{
		$year = substr($n, 0, 4);
		$month = substr($n, 4, 2);
		$day = substr($n, 6, 2);

		return $year . '-' . $month . '-' . $day;
	}

	private static function sortPathDailyVisits(array $pathVisits): array
	{
		if (1 === count($pathVisits)) {
			return $pathVisits;
		}

		$minDayNumber = null;
		$maxDayNumber = null;
		foreach ($pathVisits as $dayNumber => $dayVisitsCount) {
			if ($minDayNumber === null || $minDayNumber > $dayNumber) {
				$minDayNumber = $dayNumber;
			}
			if ($maxDayNumber === null || $maxDayNumber < $dayNumber) {
				$maxDayNumber = $dayNumber;
			}
		}

		$deltaDays = $maxDayNumber - $minDayNumber;
		$allDaysPathVisits = array_fill(0, $deltaDays, 0);
		foreach ($pathVisits as $day => $dayVisitsCount) {
			$allDaysPathVisits[$day - $minDayNumber] = $dayVisitsCount;
		}

		$sortedPathVisits = [];
		foreach ($allDaysPathVisits as $day => $dayVisitsCount) {
			if (0 !== $dayVisitsCount) {
				$sortedPathVisits[$day + $minDayNumber] = $dayVisitsCount;
			}
		}

		return $sortedPathVisits;
	}
}