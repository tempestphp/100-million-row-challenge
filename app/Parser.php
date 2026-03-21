<?php

namespace App;

use App\Commands\Visit;

final class Parser {
	private const int URI_PREFIX_LEN = 25;	// "https://stitcher.io/blog/"
	private const int READ_CHUNK_SIZE = 1_048_576;
	private const int NEXT_PATH_OFFSET = 52;

	private static string $input_path = '';
	private static string $output_path = '';

	private static ?array $date_list = null;
	private static ?array $date_id_map = null;
	private static ?array $date_prefixes = null;
	private static int $date_count = 0;

	private static ?array $path_base_map = null;
	private static ?array $path_id_map = null;
	private static ?array $path_prefixes = null;
	private static int $path_count = 0;

	public function parse(string $input_path, string $output_path): void {
		gc_disable();

		Parser::buildDateList();
		Parser::buildPathMap();

		Parser::$input_path = $input_path;
		Parser::$output_path = $output_path;

		$counts = array_fill(0, Parser::$path_count * Parser::$date_count, 0);

		// count visits and collect paths in first-appearance order from the csv
		$encountered_paths = Parser::countVisits($counts);

		Parser::writePrettyJson($counts, $encountered_paths);
	}

	// build a list of all the dates we'll be counting - in our case the start of 2021 to the end of 2026
	private static function buildDateList(): void {
		if (!empty(Parser::$date_list) && !empty(Parser::$date_id_map)) {
			return;
		}

		Parser::$date_list = [];
		Parser::$date_id_map = [];
		Parser::$date_prefixes = [];

		for ($year = 2021; $year <= 2026; ++$year) {
			for ($month = 1; $month <= 12; ++$month) {
				$max_day = match ($month) {
					2 => Parser::isLeapYear($year) ? 29 : 28,
					4, 6, 9, 11 => 30,
					default => 31,
				};

				for ($day = 1; $day <= $max_day; ++$day) {
					$date =
						$year . '-' .
						($month < 10 ? '0' : '') . $month . '-' .
						($day < 10 ? '0' : '') . $day;

					Parser::$date_list[Parser::$date_count] = $date;
					Parser::$date_id_map[$date] = Parser::$date_count;
					Parser::$date_prefixes[Parser::$date_count] = "        \"" . $date . '": ';
					++Parser::$date_count;
				}
			}
		}
	}

	private static function buildPathMap()
	{
		if (!empty(Parser::$path_base_map) && !empty(Parser::$path_id_map)) {
			return;
		}

		Parser::$path_base_map = [];
		Parser::$path_id_map = [];
		Parser::$path_prefixes = [];

		foreach (Visit::all() as $visit) {
			$path = substr($visit->uri, self::URI_PREFIX_LEN);
			Parser::$path_base_map[$path] = Parser::$path_count * Parser::$date_count;
			Parser::$path_id_map[$path] = Parser::$path_count;
			Parser::$path_prefixes[Parser::$path_count] = "    \"\/blog\/" . str_replace('/', '\/', $path) . "\": {\n";
			Parser::$path_count++;
		}
	}

	private static function isLeapYear(int $year): bool {
		return ($year % 4 === 0 && $year % 100 !== 0) || ($year % 400 === 0);
	}

	// count the visits to each blogpost and record the order in which paths first appeared in the csv
	private static function countVisits(array &$counts): array {
		$input = fopen(Parser::$input_path, 'r');

		stream_set_read_buffer($input, 0);

		$tail = '';
		$seen_paths = [];
		$encountered_paths = [];
		$path_cache = [];

		// optimization - read 1mb of data every time
		while (!feof($input)) {
			$buffer = fread($input, Parser::READ_CHUNK_SIZE);

			if ($buffer === '') {
				break;
			}

			if ($tail !== '') {
				$buffer = $tail . $buffer;
			}

			$last_newline = strrpos($buffer, "\n");
			if ($last_newline === false) {
				$tail = $buffer;
				continue;
			}

			$chunk_len = $last_newline;
			$p = self::URI_PREFIX_LEN;
			$fence = $chunk_len - 1024;

			while ($p < $fence) {
				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;

				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
				$p = $sep + self::NEXT_PATH_OFFSET;
			}

			// safe chunk for the fallback's end
			while ($p < $chunk_len) {
				$sep = strpos($buffer, ',', $p);
				if ($sep === false || $sep >= $chunk_len) {
					break;
				}

				$path = substr($buffer, $p, $sep - $p);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);
				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($buffer, $sep + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}

				$p = $sep + self::NEXT_PATH_OFFSET;
			}

			$tail = substr($buffer, $last_newline + 1);
		}

		if ($tail !== '') {
			$comma = strpos($tail, ',');
			if ($comma !== false && $comma > self::URI_PREFIX_LEN) {
				$path = substr($tail, self::URI_PREFIX_LEN, $comma - self::URI_PREFIX_LEN);
				$path_base = $path_cache[$path] ??= (Parser::$path_base_map[$path] ?? null);

				if ($path_base !== null) {
					if (!isset($seen_paths[$path])) {
						$seen_paths[$path] = true;
						$encountered_paths[] = $path;
					}

					$date = substr($tail, $comma + 1, 10);
					$date_id = Parser::$date_id_map[$date] ?? null;
					if ($date_id !== null) {
						++$counts[$path_base + $date_id];
					}
				}
			}
		}

		fclose($input);

		return $encountered_paths;
	}

	private static function writePrettyJson(array $counts, array $encountered_paths): void {
		$output = fopen(Parser::$output_path, 'w');

		fwrite($output, "{\n");

		$separator = '';

		foreach ($encountered_paths as $path) {
			$path_id = Parser::$path_id_map[$path];
			$base = $path_id * Parser::$date_count;

			$first_date_id = -1;
			for ($date_id = 0; $date_id < Parser::$date_count; ++$date_id) {
				if ($counts[$base + $date_id] !== 0) {
					$first_date_id = $date_id;
					break;
				}
			}

			if ($first_date_id === -1) {
				continue;
			}

			$buffer = $separator;
			$buffer .= Parser::$path_prefixes[$path_id];
			$buffer .= Parser::$date_prefixes[$first_date_id] . $counts[$base + $first_date_id];

			for ($date_id = $first_date_id + 1; $date_id < Parser::$date_count; ++$date_id) {
				$count = $counts[$base + $date_id];
				if ($count === 0) {
					continue;
				}

				$buffer .= ",\n";
				$buffer .= Parser::$date_prefixes[$date_id] . $count;
			}

			$buffer .= "\n    }";

			fwrite($output, $buffer);
			$separator = ",\n";
		}

		fwrite($output, "\n}");
		fclose($output);
	}
}
