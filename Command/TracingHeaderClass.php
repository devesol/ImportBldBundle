<?php

/*
 *  * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace FCS\ImportBldBundle\Command;

/**
 * Description of TracingHeaderClass
 *
 * @author cdelamarre
 */
class TracingHeaderClass {

    private $lineToAnalyse;
    private $filePath;
    private $shptRef;
    private $whseDate;
    private $whseTime;
    private $aDetail = array();
    private $output;

    public function __construct() {

        $a = func_get_args();
        $i = func_num_args();
        if (method_exists($this, $f = '__construct' . $i)) {
            call_user_func_array(array($this, $f), $a);
        }
    }

    public function __construct1($lineToAnalyse) {
        $this->lineToAnalyse = $lineToAnalyse;
    }

    public function setLineToAnalyse($x) {
        $this->lineToAnalyse = $x;
        $this->init();
    }

    public function setFilePath($x) {
        $this->filePath = $x;
    }

    public function getFilePath() {
        return $this->filePath;
    }

    public function getADetail() {
        return $this->aDetail;
    }

    public function arrayDetailPush($oDetail) {
        array_push($this->aDetail, $oDetail);
    }

    private function getLineToAnalyse() {
        return $this->lineToAnalyse;
    }

    private function init() {
       
        $this->setShptRef($this->lineToAnalyse);
        $this->setWhseDate($this->lineToAnalyse);
        $this->setWhseTime($this->lineToAnalyse);
    }

    private function setShptRef($x) {
        $this->shptRef = substr($x, 41, 11);
    }

    public function getShptRef() {
        return $this->shptRef;
    }

    private function setWhseDate($x) {
        $this->whseDate = substr($x, 56, 8);
    }

    public function getWhseDate() {
        return $this->whseDate;
    }

    private function setWhseTime($x) {
        $this->whseTime = substr($x, 64, 4);
    }

    public function getWhseTime() {
        return $this->whseTime;
    }

    public function printAll() {
        print "filePath:" . $this->filePath . "\n";
        print "shptRef:" . $this->shptRef . "\n";
        print "whseDate:" . $this->whseDate . "\n";
        print "whseTime :" . $this->whseTime . "\n";
    }

}
