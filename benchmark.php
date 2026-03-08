<?php

error_reporting(0);

$iterations = 100000000;

echo "=== Array Access vs Variable Variables ===\n\n";

$a = [];
$a['foo'] = 0;

$t0 = microtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $a['foo']++;
}
$t1 = microtime(true);
echo "Single array:   " . number_format($t1 - $t0, 4) . "s val:".$a['foo'] ." \n";

$a = [];
$a['foo'] = [];
$a['foo']['2024-01-15'] = 0;

$t0 = microtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $a['foo']['2024-01-15']++;
}
$t1 = microtime(true);
echo "Nested array:   " . number_format($t1 - $t0, 4) . "s val:".$a['foo']['2024-01-15'] ." \n";

$b = '/foo/asdf/wer.html,2024-01-15';

$t0 = microtime(true);
for ($i = 0; $i < $iterations; $i++) {
    $$b++;
}
$t1 = microtime(true);
echo "variable variables:   " . number_format($t1 - $t0, 4) . "s val:". $$b ."\n";
$arr = get_defined_vars();
print_r($arr);

