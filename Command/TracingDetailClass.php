<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace FCS\ImportBldBundle\Command;

/**
 * Description of TracingHeaderClass
 *
 * @author cdelamarre
 */
class TracingDetailClass {

    private $sku;
    private $poRoot;
    private $numPoste;
    private $codePays;
    private $ctn;
    private $pcs;
    private $reseauBld;

    public function __construct() {
        $a = func_get_args();
        $i = func_num_args();
        if (method_exists($this, $f = '__construct' . $i)) {
            call_user_func_array(array($this, $f), $a);
        }
    }

    function __construct1($lineToAnalyse) {
        $this->lineToAnalyse = $lineToAnalyse;
        $this->setLineToAnalyse($lineToAnalyse);
    }

    public function setLineToAnalyse($x) {
        $this->lineToAnalyse = $x;
        $this->init();
    }

    public function init() {
        $this->setSku($this->lineToAnalyse);
        $this->setPoRoot($this->lineToAnalyse);
        $this->setNumPoste($this->lineToAnalyse);
        $this->setCodePays($this->lineToAnalyse);
        $this->setCtn($this->lineToAnalyse);
        $this->setPcs($this->lineToAnalyse);
        $this->setReseauBld($this->lineToAnalyse);
    }

    private function setSku($x) {
        $this->sku = substr($x, 11, 15);
    }

    private function setPoRoot($x) {
        $this->poRoot = substr($x, 26, 10);
    }

    private function setNumPoste($x) {
        $this->numPoste = substr($x, 37, 2);
    }

    private function setCodePays($x) {
        $this->codePays = substr($x, 74, 2);
    }

    private function setCtn($x) {
        $this->ctn = substr($x, 82, 7);
    }

    private function setPcs($x) {
        $this->pcs = substr($x, 89, 10);
    }

    private function setReseauBld($x) {
        $this->reseauBld = substr($x, 99, 3);
    }

    public function getSku() {
        return $this->sku;
    }

    public function getPoRoot() {
        return $this->poRoot;
    }

    public function getNumPoste() {
        return $this->numPoste;
    }

    public function getCodePays() {
        return $this->codePays;
    }

    public function getCtn() {
        return $this->ctn;
    }

    public function getPcs() {
        return $this->pcs;
    }

    public function getReseauBld() {
        return $this->reseauBld;
    }

    public function printAll() {
        print "sku:" . $this->sku . "\n";
        print "poRoot:" . $this->poRoot . "\n";
        print "numPoste:" . $this->numPoste . "\n";
        print "codePays:" . $this->codePays . "\n";
        print "ctn:" . $this->ctn . "\n";
        print "pcs:" . $this->pcs . "\n";
        print "reseauBld:" . $this->reseauBld . "\n";
    }

}
