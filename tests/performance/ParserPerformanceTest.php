<?php

declare(strict_types=1);

namespace Tests\Performance;

use App\Parser;
use PHPUnit\Framework\TestCase;

/**
 * Performance test harness for Parser class.
 * These tests measure execution time and memory usage.
 * They are not typical unit tests but performance benchmarks.
 */
class ParserPerformanceTest extends TestCase
{
    private Parser $parser;
    
    protected function setUp(): void
    {
        $this->parser = new Parser();
    }
    
    /**
     * Test baseline performance with small dataset
     * This establishes a performance baseline for regression testing
     */
    public function test_small_dataset_performance(): void
    {
        $input = __DIR__ . '/../../data/test-data.csv';
        $output = __DIR__ . '/../../data/test-output-performance.json';
        
        $start = microtime(true);
        $this->parser->parse($input, $output);
        $end = microtime(true);
        
        $time = $end - $start;
        $memory = memory_get_peak_usage(true);
        
        // Assert performance thresholds (adjust based on your requirements)
        $this->assertLessThan(0.1, $time, 'Parsing should complete in under 0.1 seconds for 1k rows');
        $this->assertLessThan(50 * 1024 * 1024, $memory, 'Memory usage should be under 50MB for 1k rows');
        
        // Output metrics for monitoring
        echo sprintf("\nSmall dataset (1k rows): %.3f seconds, %.2f MB peak memory\n", 
            $time, $memory / 1024 / 1024);
        
        // Clean up
        if (file_exists($output)) {
            unlink($output);
        }
    }
    
    /**
     * Test performance with large dataset (100k rows)
     */
    public function test_large_dataset_performance(): void
    {
        $input = __DIR__ . '/../../data/large-test.csv';
        $output = __DIR__ . '/../../data/large-output-performance.json';
        
        $start = microtime(true);
        $this->parser->parse($input, $output);
        $end = microtime(true);
        
        $time = $end - $start;
        $memory = memory_get_peak_usage(true);
        
        // Assert performance thresholds
        $this->assertLessThan(1.0, $time, 'Parsing should complete in under 1 second for 100k rows');
        $this->assertLessThan(200 * 1024 * 1024, $memory, 'Memory usage should be under 200MB for 100k rows');
        
        // Output metrics for monitoring
        echo sprintf("\nLarge dataset (100k rows): %.3f seconds, %.2f MB peak memory\n", 
            $time, $memory / 1024 / 1024);
        
        // Clean up
        if (file_exists($output)) {
            unlink($output);
        }
    }
    
    /**
     * Test memory scalability - memory should not grow linearly with row count
     * This is a more advanced test that would need actual implementation
     */
    public function test_memory_scalability(): void
    {
        $this->markTestSkipped('Memory scalability test requires chunked processing implementation');
        // Future implementation: test that memory usage grows sub-linearly
    }
    
    /**
     * Test correctness alongside performance
     */
    public function test_correctness_with_performance(): void
    {
        $input = __DIR__ . '/../../data/test-data.csv';
        $output = __DIR__ . '/../../data/test-output-correctness.json';
        
        $this->parser->parse($input, $output);
        
        // Validate output matches expected
        $expected = json_decode(file_get_contents(__DIR__ . '/../../data/test-data-expected.json'), true);
        $actual = json_decode(file_get_contents($output), true);
        
        $this->assertEquals($expected, $actual, 'Parser output should match expected result');
        
        // Clean up
        if (file_exists($output)) {
            unlink($output);
        }
    }
}
