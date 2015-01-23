<?php namespace Illuminate\Database\Connectors;

use PDO;
use Illuminate\Container\Container;
use Illuminate\Database\MySqlConnection;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Database\PostgresConnection;
use Illuminate\Database\SqlServerConnection;

class ConnectionFactory {

	/**
	 * The IoC container instance.
	 *
	 * @var \Illuminate\Container\Container
	 */
	protected $container;

	/**
	 * Create a new connection factory instance.
	 *
	 * @param  \Illuminate\Container\Container  $container
	 * @return void
	 */
	public function __construct(Container $container)
	{
		$this->container = $container;
	}

	/**
	 * Establish a PDO connection based on the configuration.
	 *
	 * @param  array   $config
	 * @param  string  $name
	 * @return \Illuminate\Database\Connection
	 */
	public function make(array $config, $name = null)
	{
		$config = $this->parseConfig($config, $name);

		return $this->createBlankConnection($config);
	}

	/**
	 * Create a blank connection
	 * 
	 * @param array config
	 * @return \Illuminate\Database\Connection
	 */
	protected function createBlankConnection(array $config)
	{
		return $this->createConnection($config['driver'], $config['database'], $config['prefix'], $config, $this);
	}


	/**
	 * Create a single database connection instance.
	 *
	 * @param  array  $config
	 * @return \Illuminate\Database\Connection
	 */
	public function createReadConnection(array $config)
	{
		$readConfig = $this->getReadConfig($config);

		return $this->createConnector($readConfig)->connect($readConfig);
	}

	/**
	 * Create a single database connection instance.
	 *
	 * @param  array  $config
	 * @return \Illuminate\Database\Connection
	 */
	public function createWriteConnection(array $config)
	{
		$writeConfig = $this->getWriteConfig($config);

		return $this->createConnector($writeConfig)->connect($writeConfig);
	}

	/**
	 * Get the read configuration for a read connection.
	 *
	 * @param  array  $config
	 * @return array
	 */
	protected function getReadConfig(array $config)
	{
		$readConfig = $this->getReadWriteConfig($config, 'read');

		return $this->mergeReadWriteConfig($config, $readConfig);
	}

	/**
	 * Get the read configuration for a write connection.
	 *
	 * @param  array  $config
	 * @return array
	 */
	protected function getWriteConfig(array $config)
	{
		$writeConfig = $this->getReadWriteConfig($config, 'write');

		return $this->mergeReadWriteConfig($config, $writeConfig);
	}

	/**
	 * Get a read / write level configuration.
	 *
	 * @param  array   $config
	 * @param  string  $type
	 * @return array
	 */
	protected function getReadWriteConfig(array $config, $type)
	{
		// This is for multiple databases of a single type
		if (isset($config[$type][0]))
		{
			return $config[$type][array_rand($config[$type])];
		}

		// Return the specific config type, or fall back to the basic config structure
		return (isset($config[$type])) ? $config[$type] : $config;
	}

	/**
	 * Merge a configuration for a read / write connection.
	 *
	 * @param  array  $config
	 * @param  array  $merge
	 * @return array
	 */
	protected function mergeReadWriteConfig(array $config, array $merge)
	{
		return array_except(array_merge($config, $merge), array('read', 'write'));
	}

	/**
	 * Parse and prepare the database configuration.
	 *
	 * @param  array   $config
	 * @param  string  $name
	 * @return array
	 */
	protected function parseConfig(array $config, $name)
	{
		return array_add(array_add($config, 'prefix', ''), 'name', $name);
	}

	/**
	 * Create a connector instance based on the configuration.
	 *
	 * @param  array  $config
	 * @return \Illuminate\Database\Connectors\ConnectorInterface
	 *
	 * @throws \InvalidArgumentException
	 */
	public function createConnector(array $config)
	{
		if ( ! isset($config['driver']))
		{
			throw new \InvalidArgumentException("A driver must be specified.");
		}

		if ($this->container->bound($key = "db.connector.{$config['driver']}"))
		{
			return $this->container->make($key);
		}

		switch ($config['driver'])
		{
			case 'mysql':
				return new MySqlConnector;

			case 'pgsql':
				return new PostgresConnector;

			case 'sqlite':
				return new SQLiteConnector;

			case 'sqlsrv':
				return new SqlServerConnector;
		}

		throw new \InvalidArgumentException("Unsupported driver [{$config['driver']}]");
	}

	/**
	 * Create a new connection instance.
	 *
	 * @param  string   $driver
	 * @param  string   $database
	 * @param  string   $prefix
	 * @param  array    $config
	 * @return \Illuminate\Database\Connection
	 *
	 * @throws \InvalidArgumentException
	 */
	protected function createConnection($driver, $database, $prefix = '', array $config = array())
	{
		if ($this->container->bound($key = "db.connection.{$driver}"))
		{
			return $this->container->make($key, array($connection, $database, $prefix, $config));
		}

		switch ($driver)
		{
			case 'mysql':
				return new MySqlConnection($database, $prefix, $config, $this);

			case 'pgsql':
				return new PostgresConnection($database, $prefix, $config, $this);

			case 'sqlite':
				return new SQLiteConnection($database, $prefix, $config, $this);

			case 'sqlsrv':
				return new SqlServerConnection($database, $prefix, $config, $this);
		}

		throw new \InvalidArgumentException("Unsupported driver [$driver]");
	}

}
