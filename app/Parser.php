<?php declare(strict_types=1);

namespace App;



final class Parser
{
	private const SHM_SIZE = 262144 / 2;
	private const OFFSET_STATUS = 0;
	private const OFFSET_LENGTH = 4;
	private const OFFSET_DATA = 8;

	private const STATUS_EMPTY = 0;
	private const STATUS_FULL = 1;
	private const STATUS_DONE = 2;

	public function parse(string $inputPath, string $outputPath): void
	{

		\gc_disable();

		$numWorkers = 4;
		$outFd      = \fopen($outputPath, 'wb');
		\stream_set_write_buffer($outFd, 1024 * 1024 * 10);
		\fwrite($outFd, "{\n");

		$lockFile   = \sys_get_temp_dir() . '/_parser_100m_.lock';
		\file_put_contents($lockFile, '0');

		$shmIds = [];
		$pids   = [];

		try {

			if ($_SERVER['argv'][1] === 'data:validate') {
				$numWorkers = 1;
			}

			$keyBase = \ftok(__FILE__, 'p');
			for ($i = 0; $i < $numWorkers; $i++)
			{
				$shmId      = \shmop_open($keyBase + $i, "n", 0666, self::SHM_SIZE);
				$shmIds[$i] = $shmId;

				\shmop_write($shmId, \pack('I', self::STATUS_EMPTY), self::OFFSET_STATUS);
			}

			for ($i = 0; $i < $numWorkers; $i++)
			{
				$pid = \pcntl_fork();

				if ($pid === 0) {

					$this->worker($shmIds[$i], $outFd, $lockFile);
					exit(0);

				} elseif ($pid === -1) {

					throw new \Exception("Fork failed");

				} else {

					$pids[$i] = $pid;
				}
			}

			$this->lb($inputPath, $shmIds, $numWorkers);

			foreach ($pids as $pid)
			{
				\pcntl_waitpid($pid, $status);
			}

			\fwrite($outFd, "\n}");
			\fclose($outFd);

		} finally {

			for ($i = 0; $i < $numWorkers; $i++)
			{
				\shmop_delete($shmIds[$i]);
			}
			\unlink($lockFile);
		}
	}


	private function lb(string $inputPath, array $shmIds, int $numWorkers): void
	{
		$handle   = \fopen($inputPath, 'rb');
		\stream_set_read_buffer($handle, 1024 * 1024 * 3);

		$buffers  = \array_fill(0, $numWorkers, '');
		$lines    = \array_fill(0, $numWorkers, 0);
		// $maxChunk = 13777;
		$maxLines = 1000;

		while (($line = \fgets($handle)) !== false)
		{
			$commaPos  = \strpos($line, ',');
			$pathStart = \strpos($line, '/', 8);
			$workerId  = (\crc32(\substr($line, $pathStart, $commaPos - $pathStart)) & 0xffffffff) % $numWorkers;

			$buffers[$workerId] .= $line;
			$lines[$workerId]++;

			if ($lines[$workerId] >= $maxLines) {

			// if (strlen($buffers[$workerId]) >= $maxChunk) {
				$this->writeChunk($shmIds[$workerId], $buffers[$workerId]);
				$buffers[$workerId] = '';
				$lines[$workerId] = 0;
			}
		}

		foreach ($buffers as $i => $buf)
		{
			if ($buf !== '') {
				$this->writeChunk($shmIds[$i], $buf);
			}
		}

		for ($i = 0; $i < $numWorkers; $i++)
		{
			$this->signalEof($shmIds[$i]);
		}

		\fclose($handle);
	}

	private function writeChunk($shmId, string $data): void
	{
		while (true)
		{
			$status = \unpack('I', \shmop_read($shmId, self::OFFSET_STATUS, 4))[1];
			if ($status === self::STATUS_EMPTY) break;
			\usleep(70);
		}

		\shmop_write($shmId, \pack('I', \strlen($data)), self::OFFSET_LENGTH);
		\shmop_write($shmId, $data, self::OFFSET_DATA);
		\shmop_write($shmId, \pack('I', self::STATUS_FULL), self::OFFSET_STATUS);
	}

	private function signalEof($shmId): void
	{
		while (true) {
			$status = \unpack('I', \shmop_read($shmId, self::OFFSET_STATUS, 4))[1];
			if ($status === self::STATUS_EMPTY) break;
			\usleep(70);
		}

		\shmop_write($shmId, \pack('I', 0), self::OFFSET_LENGTH); // len 0
		\shmop_write($shmId, \pack('I', self::STATUS_DONE), self::OFFSET_STATUS);
	}

	private function worker($shmId, $outFd, string $lockFile): void
	{
		$aggregates = [];
		$buffer     = '';
		while (true)
		{
			$status = 0;
			while (true)
			{
				$status = \unpack('I', \shmop_read($shmId, self::OFFSET_STATUS, 4))[1];
				if ($status === self::STATUS_FULL || $status === self::STATUS_DONE) break;
				\usleep(70);
			}

			$len = \unpack('I', \shmop_read($shmId, self::OFFSET_LENGTH, 4))[1];

			if ($status === self::STATUS_DONE || $len === 0) {
				\shmop_write($shmId, \pack('I', self::STATUS_EMPTY), self::OFFSET_STATUS);
				break;
			}

			// read data
			$data = \shmop_read($shmId, self::OFFSET_DATA, $len);

			\shmop_write($shmId, \pack('I', self::STATUS_EMPTY), self::OFFSET_STATUS);

			foreach (\explode("\n", $data) as $line)
			{
				if ($line !== '') {
					$commaPos  = \strrpos($line, ',');
					$pathStart = \strpos($line, '/', 8);
					$path      = \substr($line, $pathStart, $commaPos - $pathStart);
					$date      = \substr($line, $commaPos + 1, 10);

					if (isset($aggregates[$path][$date])) {
						$aggregates[$path][$date]++;
					} else {
						$aggregates[$path][$date] = 1;
					}
				}
			}
		}

		if (empty($aggregates)) return;

		foreach ($aggregates as $path => &$dates) \ksort($dates);
		unset($dates);

		$lockFd = \fopen($lockFile, 'rb+');
		\flock($lockFd, LOCK_EX);
		\rewind($lockFd);

		$hasEntries = (int) \fgets($lockFd);

		foreach ($aggregates as $path => $dates)
		{
			if ($hasEntries) {
				\fwrite($outFd, ",\n");
			} else {
				$hasEntries = 1;
			}

			$json = \json_encode([$path => $dates], \JSON_PRETTY_PRINT);
			$json = \explode("\n", $json);
			\array_shift($json); // remove {
			\array_pop($json);   // remove }

			\fwrite($outFd, \implode("\n", $json));
		}


		\fflush($outFd);

		\rewind($lockFd);
		\fwrite($lockFd, "1\n");

		\flock($lockFd, LOCK_UN);
		\fclose($lockFd);
	}


	private function process(string $line, array &$data): void
	{
		$commaPos  = \strrpos($line, ',');
		$protoEnd  = \strpos($line, '://');
		$pathStart = \strpos($line, '/', $protoEnd + 3);
		$path      = \substr($line, $pathStart, $commaPos - $pathStart);
		$date      = \substr($line, $commaPos + 1, 10);

		if (isset($data[$path][$date])) {
			$data[$path][$date]++;
		} else {
			$data[$path][$date] = 1;
		}
	}
}
