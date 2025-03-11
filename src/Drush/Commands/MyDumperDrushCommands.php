<?php

declare(strict_types=1);

namespace Ueberbit\DrushMyDumper\Drush\Commands;

use Consolidation\OutputFormatters\StructuredData\PropertyList;
use Consolidation\SiteAlias\SiteAliasManagerInterface;
use DrupalFinder\DrupalFinder;
use DrupalFinder\DrupalFinderComposerRuntime;
use Drush\Attributes as CLI;
use Drush\Boot\DrupalBootLevels;
use Drush\Commands\AutowireTrait;
use Drush\Commands\DrushCommands;
use Drush\Commands\sql\SqlCommands;
use Drush\Exec\ExecTrait;
use Drush\Sql\SqlBase;
use Drush\Sql\SqlMysql;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\DependencyInjection\Attribute\Autowire;

#[CLI\Bootstrap(DrupalBootLevels::NONE)]
class MyDumperDrushCommands extends DrushCommands {

  use AutowireTrait;
  use ExecTrait;

  private DrupalFinderComposerRuntime $drupalFinder;

  public function __construct(
    private readonly SiteAliasManagerInterface $siteAliasManager
  ) {
    parent::__construct();
    $this->drupalFinder = new DrupalFinderComposerRuntime();
  }

  #[CLI\Command(name: 'mydumper')]
  #[CLI\Bootstrap(level: DrupalBootLevels::MAX, max_level: DrupalBootLevels::CONFIGURATION)]
  #[CLI\Option(name: 'directory', description: "The output directory")]
  #[CLI\Option(name: 'database', description: 'The DB connection key if using multiple connections in settings.php.')]
  #[CLI\Option(name: 'target', description: 'The name of a target within the specified database connection.')]
  #[CLI\OptionsetTableSelection]
  #[CLI\FieldLabels(labels: ['path' => 'Path'])]
  public function mydumper($options = [
    'directory' => self::REQ,
    'format' => 'null',
  ]
  ): PropertyList {
    if ($options['tables-key'] !== NULL) {
      throw new InvalidArgumentException('--tables-key option is not supported.');
    }
    if ($options['tables-list'] !== NULL) {
      throw new InvalidArgumentException('--tables-list option is not supported.');
    }
    if ($options['database'] === FALSE) {
      unset($options['database']);
    }
    if ($options['target'] === FALSE) {
      unset($options['target']);
    }
    if ($options['directory'] === NULL) {
      $options['directory'] = $this->drupalFinder->getComposerRoot() . '/export-' . date('Ymd') . '-' . date('His');
    }
    $sql = SqlBase::create($options);
    if (!($sql instanceof SqlMysql)) {
      throw new \RuntimeException('Database driver not supported.');
    }

    $table_selection = $sql->getExpandedTableSelection($options, $sql->listTables());
    $exclude = array_merge($table_selection['skip'], $table_selection['structure']);
    $exclude = array_unique($exclude);

    if ($exclude) {
      $exclude = array_map(function ($table) use ($sql) {
        return $sql->getDbSpec()['database'] . '.' . $table;
      }, $exclude);
      $excludeFile = drush_save_data_to_temp_file(implode(PHP_EOL, $exclude));
    }

    // Dump data.
    $cmd = [
      'mydumper',
    ];
    $creds = $this->convertCommandLineParameters(explode(' ', $sql->creds()));
    $args = [];
    if (isset($options['directory'])) {
      $args[] = '--outputdir=' . $options['directory'];
    }
    if (isset($excludeFile)) {
      $args[] = '--omit-from-file=' . $excludeFile;
    }
    $cmd = array_merge($cmd, $creds, $args);
    $process = $this->processManager()->process($cmd);

    // Avoid the php memory of saving stdout.
    // Show dump in real-time on stdout, for backward compat.
    $process->mustRun($process->showRealtime());
    $metadataDataDump = $this->parseMyDumperMetadata($options['directory'] . '/metadata');

    // Dump schema.
    if ($table_selection['structure']) {
      $tableList = array_map(function ($table) use ($sql) {
        return $sql->getDbSpec()['database'] . '.' . $table;
      }, $table_selection['structure']);
      $cmd = [
        'mydumper',
      ];
      $creds = $this->convertCommandLineParameters(explode(' ', $sql->creds()));
      $args = [
        '--dirty',
        '--outputdir=' . $options['directory'],
        '--tables-list=' . implode(',', $tableList),
        '--no-data',
      ];
      $cmd = array_merge($cmd, $creds, $args);
      $process = $this->processManager()->process($cmd);
      // Avoid the php memory of saving stdout.
      // Show dump in real-time on stdout, for backward compat.
      $process->mustRun($process->showRealtime());
      $metadataSchemaDump = $this->parseMyDumperMetadata($options['directory'] . '/metadata');
    }

    if (isset($metadataDataDump) && isset($metadataSchemaDump)) {
      file_put_contents($options['directory'] . '/metadata', $this->writeMetadata($this->mergeMetadata($metadataDataDump, $metadataSchemaDump)));
    }

    // SqlBase::dump() returns null if 'result-file' option is empty.
    $this->logger()
      ->success(dt('Database dump saved to !path', ['!path' => $options['directory']]));
    return new PropertyList(['path' => $options['directory']]);
  }

  #[CLI\Command(name: 'myloader')]
  #[CLI\Bootstrap(level: DrupalBootLevels::MAX, max_level: DrupalBootLevels::CONFIGURATION)]
  #[CLI\Option(name: 'directory', description: "The output directory")]
  #[CLI\Option(name: 'database', description: 'The DB connection key if using multiple connections in settings.php.')]
  #[CLI\Option(name: 'target', description: 'The name of a target within the specified database connection.')]
  public function myloader($options = ['directory' => self::REQ]) {
    if ($options['database'] === FALSE) {
      unset($options['database']);
    }
    if ($options['target'] === FALSE) {
      unset($options['target']);
    }
    $sql = SqlBase::create($options);
    if (!($sql instanceof SqlMysql)) {
      throw new \RuntimeException('Database driver not supported.');
    }

    $self = $this->siteAliasManager->getSelf();
    $process = $this->processManager()->drush($self, SqlCommands::DROP);
    $process->run($process->showRealtime());

    $cmd = [
      'myloader',
    ];
    $creds = $this->convertCommandLineParameters(explode(' ', $sql->creds()));
    $args = [
      '--directory=' . $options['directory'],
    ];

    $cmd = array_merge($cmd, $creds, $args);
    $process = $this->processManager()->process($cmd);

    // Avoid the php memory of saving stdout.
    // Show dump in real-time on stdout, for backward compat.
    $process->run($process->showRealtime());
  }

  protected function parseMyDumperMetadata(string $filePath): array {
    $file = file($filePath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    $metadata = [];
    $group = NULL;

    foreach ($file as $line) {
      if (str_starts_with($line, '#')) {
        continue;
      }
      if (str_starts_with($line, '[')) {
        $group = $line;
        $metadata[$group] = $metadata[$group] ?? [];
        continue;
      }
      if ($group === NULL) {
        throw new \LogicException('Invalid mydumper metadata file.');
      }
      $metadata[$group][] = $line;
    }

    return $metadata;
  }

  protected function mergeMetadata(...$metadata): array {
    // Keep config and myloader_session_variables from first item, remove from all others. Merge in others.
    $first = $metadata[0];
    $other = array_slice($metadata, 1);

    foreach ($other as &$item) {
      unset($item['[config]'], $item['[myloader_session_variables]']);
    }

    return array_merge($first, ...$other);
  }

  protected function writeMetadata(array $metadata): string {
    $content = [];
    foreach ($metadata as $group => $lines) {
      $content = array_merge($content, [$group], $lines, ['']);
    }
    return implode(PHP_EOL, $content);
  }

  protected function convertCommandLineParameters(array $parameters): array {
    $replace = [
      '--ssl-ca=' => '--ca=',
      '--ssl-capath=' => '--capath=',
      '--ssl-cert=' => '--cert=',
      '--ssl-cipher=' => '--cipher=',
      '--ssl-key=' => '--key=',
    ];
    $sslEnabled = FALSE;
    $parameters = array_map(function ($argument) use ($replace, &$sslEnabled) {
      $result = str_replace(array_keys($replace), array_values($replace), $argument, $count);
      if ($count > 0) {
        $sslEnabled = TRUE;
      }
      return $result;
    }, $parameters);

    if ($sslEnabled) {
      $parameters[] = '--ssl';
    }

    return $parameters;
  }

}
