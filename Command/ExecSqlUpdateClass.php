<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace FCS\ImportBldBundle\Command;

use M1\Vars\Vars;

/**
 * Description of ExecSqlUpdateClass
 *
 * @author cdelamarre
 */
class ExecSqlUpdateClass {

    private $ymlConfigPath;
    private $sqlUpdatePath;
    private $dbConnection;

    public function __construct() {
        $a = func_get_args();
        $i = func_num_args();
        if (method_exists($this, $f = '__construct' . $i)) {
            call_user_func_array(array($this, $f), $a);
        }
    }

    public function __construct1($ymlConfigPath) {
        $this->ymlConfigPath = $ymlConfigPath;
        $this->init();
    }

    private function init() {
        $this->setDbConnection();
    }

    public function closeDbConnection() {
        pg_close($this->dbConnection);
    }

    private function setDbConnection() {
        $vars = new Vars(__DIR__ . '/' . $this->ymlConfigPath);
        $serverHost = $vars['parameters.database.host'];
        $serverPort = $vars['parameters.database.port'];
        $dbName = $vars['parameters.database.name'];
        $userName = $vars['parameters.database.user'];
        $password = $vars['parameters.database.password'];

        $dbConnectionString = " host=" . $serverHost;
        $dbConnectionString .= " port=" . $serverPort;
        $dbConnectionString .= " dbname=" . $dbName;
        $dbConnectionString .= " user=" . $userName;
        $dbConnectionString .= " password=" . $password;

        $this->dbConnection = pg_connect($dbConnectionString)
                or die('Connexion impossible : ' . pg_last_error());
    }

    public function execSqlrFromVars($sSqlrFilePath, $aVars) {
        $sqlr = $this->getSqlrFromVars($sSqlrFilePath, $aVars);
        print $this->dbConnection;
        if (pg_query($this->dbConnection, $sqlr)) {
            echo "saved";
        } else {
            echo "error insering data";
        }
    }

    public function getSqlrFromVars($sSqlrFilePath, $aVars) {
        $sqlr = file_get_contents(__DIR__ . '/' . $sSqlrFilePath, FILE_USE_INCLUDE_PATH);

        foreach ($aVars as $key => $value) {
            $sqlr = str_replace('[[' . $key . ']]', $value, $sqlr);
        }
        return $sqlr;
    }

    /*
     *     public function getJsonFromPsqlSelect() {


      $sqlr = file_get_contents(__DIR__ . '\select.sql', FILE_USE_INCLUDE_PATH);

      $result = pg_query($sqlr) or die('Echec de la requete : ' . pg_last_error());
      $resultRows = pg_num_rows($result);
      $arrayData = array();
      while ($data = pg_fetch_object($result)) {
      $tmpData = array();
      $vars = get_object_vars($data);
      foreach ($vars as $key => $var) {
      $tmpData["\"" . $key . "\""] = $data->$key;
      }
      array_push($arrayData, $tmpData);
      }

      pg_free_result($result);
      pg_close($dbconn);

      $arrayMain = array(
      "\"results\"" => $arrayData,
      );
      $strJson = json_encode($arrayMain, JSON_PRETTY_PRINT);
      $output->writeln($strJson);
      }
     */
}
