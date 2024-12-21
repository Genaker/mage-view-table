<?php

namespace Mage\ViewTable\Setup;

use function Mage\DB2\formatTime;
use Illuminate\Database\QueryException;
use Illuminate\Database\Schema\Blueprint;
use Magento\Framework\Setup\InstallSchemaInterface;
use Magento\Framework\Setup\ModuleContextInterface;
use Magento\Framework\Setup\SchemaSetupInterface;
use Mage\DB2\Async;
use Mage\DB2\DB2 as DB;

abstract class ViewTableCreate implements InstallSchemaInterface
{
    public $viewName;
    public $viewSQL;
    public $newTableName;

    abstract public function getSelect();

    protected function setViewName($name)
    {
        $this->viewName = $name;
    }

    public function createViewTableFromSelect($sufix = '', $fallback = true, $drop = false)
    {
        if ($drop) {
            $this->dropViewSQL($sufix);
        }
        $viewName = trim($this->viewName . "_" . $sufix, '_');
        $createViewSql = "CREATE VIEW " . $viewName . " AS " . $this->getSelect($sufix, $fallback, false);
        return $createViewSql;
    }

    public function dropViewSQL($sufix = '')
    {
        return "DROP VIEW IF EXISTS " . trim($this->viewName . '_' . $sufix, '_');
    }

    public function createTableFromView($sufix = '', $drop = false)
    {
        try {
            // Fetch columns from the view
            $columns = DB::select("DESCRIBE {$this->viewName}");
            $newTableName = trim($this->viewName . "_MVIEW_" . $sufix, '_');
            $this->newTableName = $newTableName;
            if ($drop) {
                DB::schema()->dropIfExists($newTableName);
            }
            // Start creating the new table
            DB::schema()->create($newTableName, function (Blueprint $table) use ($columns) {
                $table->id(); // Add an ID column as the primary key
                foreach ($columns as $column) {
                    $type = $this->mapColumnType($column->Type); // Map MySQL types to Laravel Schema Builder types
                    $nullable = strpos($column->Null, 'YES') !== false;

                    // Add column dynamically
                    $col = $table->$type($column->Field);
                    if ($nullable) {
                        $col->nullable();
                    }
                }
                $table->timestamps(); // Add created_at and updated_at columns
            });
        } catch (QueryException $e) {
            if ($e->getCode() === '42S01') { // Error code for "Table already exists"
                // Ignoring Table exist message

                // echo $e->getMessage();
            }
        } catch (\Exception $e) {
            throw $e;
        }
    }

    public function populateTableFromView($sufix = '', $async = false)
    {
        $async = true;
        $debug = false;
        $newTableName = trim($this->viewName . '_MVIEW_' . $sufix, '_');

        $start = microtime(true);
        $limit = 100; // Don't export everething
        DB::table(trim($this->viewName . '_' . $sufix, '_'))->orderBy('entity_id')->where('entity_id', '<', $limit) // Ensure rows are processed in a consistent order
            ->chunk(200, function ($rows) use ($newTableName, $async, &$start, $debug) {
                if ($debug) {
                    $end = microtime(true);
                    echo "View SQL Time: " . ($end - $start) . "\n";
                    $start = microtime(true);
                }
                $insertData = $rows->map(function ($row) {
                    return (array) $row; // Convert object to associative array
                })->toArray();

                if ($async) {
                    $startAwait = microtime(true);
                    $result = Async::instance()->asyncAwait();
                    $endAwait = microtime(true);
                    if ($debug) {
                        echo "Async aWait Time: " . $this->formatTime($endAwait - $startAwait) . "\n";
                    }

                    $chunks = array_chunk($insertData, 100);
                    if ($debug) {
                        echo "Chunks count: " . count($chunks) . "\n";
                    }

                    $insertSQL = [];
                    foreach ($chunks as $i => $inserts) {
                        $insertSQL[] = DB::generateInsertQuery($newTableName, $inserts);
                    }

                    $startSend = microtime(true);
                    Async::instance()->sendAsync($insertSQL, 10/*, debug: true*/, await: false);
                    $endSend = microtime(true);
                    if ($debug) {
                        echo "Send Async Time: " . $this->formatTime($endSend - $startSend) . "\n";
                    }

                } else {
                    DB::table($newTableName)->insert($insertData);
                }
                // Edge case for the last loop iteration
                Async::instance()->asyncAwait();
                $endInsert = microtime(true);
                if ($debug) {
                    echo "View SQL Insert Time: " . $this->formatTime($endInsert - $start) . "\n";
                }
                $start = microtime(true);
            });
    }

    public function gerTableName($sufix)
    {
        return trim($this->viewName . '_' . $sufix, '_');
    }

    public function formatTime($milliseconds)
    {
        return formatTime($milliseconds);
    }

    public function jsonTableCreate($tableName, $drop = false)
    {
        if ($drop) {
            DB::schema()->dropIfExists($tableName);
        }
        try {
            DB::schema()->create($tableName, function (Blueprint $table) {
                $table->id(); // Primary key
                $table->unsignedBigInteger('entity_id')->unique(); // Reference to ID
                $table->jsonb('data'); // JSONB column for searchable data
                $table->timestamps(); // Created and updated timestamps
            });
        } catch (\Exception $e) {
            throw $e;
        }
    }

    public function populateJsonTableFromView($tableName, $viewName)
    {
        //DB::connection()->enableQueryLog();
        $timeStart = microtime(false);
        // Fetch data from the view table
        DB::table($viewName)
            ->orderBy('entity_id') // Ensure consistent order (adjust based on your view structure)
            ->chunk(50, function ($rows) {
                $insertData = $rows->map(function ($row) {
                    return [
                        'id' => $row->entity_id,
                        'entity_id' => $row->entity_id, // Assuming `id` is the identifier
                        'data' => json_encode($row), // Convert entire row to JSON
                        'created_at' => date('Y-m-d H:i:s'),
                        'updated_at' => date('Y-m-d H:i:s'),
                    ];
                })->toArray();
                try {
                    // Insert chunked data into the _json table
                    DB::table($tableName)->upsert($insertData, ['entity_id'], ['data', 'updated_at']);
                } catch (\Exception $e) {
                    throw $e;
                }
            });

        // Check Table size:
        // SELECT table_name AS "Table",     round(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)" FROM      information_schema.TABLES WHERE      table_schema = "***"     AND table_name = "product_json";
        // Retrieve and print the query log
        //$queryLog = DB::connection()->getQueryLog();
        //print_r($queryLog);
        $timeEnd = microtime(false);
        // DB::flushQueryLog()
        //echo $timeEnd - $timeStart;
    }

    protected function mapColumnType($mysqlType)
    {
        if (strpos($mysqlType, 'int') !== false) {
            return 'integer';
        } elseif (strpos($mysqlType, 'varchar') !== false || strpos($mysqlType, 'text') !== false) {
            //  Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535.
            return 'text'; //'string';
        } elseif (strpos($mysqlType, 'decimal') !== false || strpos($mysqlType, 'float') !== false) {
            return 'decimal';
        } elseif (strpos($mysqlType, 'datetime') !== false) {
            return 'dateTime';
        } elseif (strpos($mysqlType, 'json') !== false) {
            return 'json';
        }
        return 'string'; // Default type
    }

    public function install(SchemaSetupInterface $setup, ModuleContextInterface $context)
    {
        $installer = $setup;
        $installer->startSetup();

        $installer->getConnection()->query($this->dropViewSQL()); // Drop the view if it exists
        $installer->getConnection()->query($this->createViewTableFromSelect()); // Create the view
        $this->createTableFromView();
        //$this->populateTableFromView();

        $installer->endSetup();
    }
}
