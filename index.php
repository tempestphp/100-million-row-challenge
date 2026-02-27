<?php

require_once 'app/Parser.php';

$script_start = microtime(true);

//file_put_contents('data/numbers.csv', implode("\n", range(100, 199)));
$p = new app\Parser();
//$p->parse('data/numbers.csv', 'data/data.json');
//$p->parse('data/test-data.csv', 'data/test-data-actual.json');
//$p->parse('/home/roey/data.csv', 'data/data.json');
//$p->parse('data/data.csv', 'data/data.json');
$p->parse('data/data-10M.csv', 'data/data.json');
//$p->parse('data/data-100M.csv', 'data/data.json');

echo "Total time: " . (microtime(true) - $script_start) . "\n";
