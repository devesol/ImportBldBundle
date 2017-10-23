<?php

namespace FCS\ImportBldBundle\Command;

use FCS\ImportBldBundle\Command\TracingHeaderClass;
use FCS\ImportBldBundle\Command\TracingDetailClass;
//use FCS\ImportBldBundle\Command\ExecSqlUpdateClass;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Finder\Finder;
use M1\Vars\Vars;

// php bin/console treatFile "../SrcImportBLD/"


require 'src/FCS/ImportBldBundle/Command/TracingHeaderClass.php';

class TreatFileCommand extends ContainerAwareCommand {

    private $srcDirectory;
    private $doneDirectory;

    protected function configure() {
        $this
                ->setName('treatFile')
                ->setDescription('List Bld Directory to import files')
                ->addArgument(
                        'srcDirectory', InputArgument::OPTIONAL, 'Argument description'
                )
                ->addOption('option', null, InputOption::VALUE_NONE, 'Option description')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output) {
        $this->init();
        $this->listenDirectory($output);
    }

    function init() {

        print $this->getYmlPathFromClassPath();
        $vars = new Vars(__DIR__ . '/' . $this->getYmlPathFromClassPath());
        $this->srcDirectory = $vars['parameters.srcDirectory'];
        $this->doneDirectory = $vars['parameters.doneDirectory'];
    }

    function getYmlPathFromClassPath() {
        $paramFilePath = __CLASS__;
        $pattern = "/.*\\\/";
        $replacement = '';
        $paramFilePath = preg_replace($pattern, $replacement, $paramFilePath);
        return $paramFilePath . ".yml";
    }

    function listenDirectory(OutputInterface $output) {
        $finder = new Finder();
        $finder
                ->files()->in($this->srcDirectory)
                ->sortByName();

        foreach ($finder as $file) {
            $pattern = '/.*YR[2|3|4]_.+/';
            if (preg_match($pattern, $file, $matches, PREG_OFFSET_CAPTURE)) {
                $this->execForEachFile($file);
            }
        }
    }

    function execForEachFile($file) {
        $oBldInput = $this->mTreatYR2YR3YR4($file);
        $this->printBldInput($oBldInput);
        $this->execSqlrUpdate($oBldInput);
        $this->mvFileToDone($file);
    }

    function mvFileToDone($srcFile) {
        $srcFile = $this->removeBackSlash($srcFile);
        if (file_exists($this->doneDirectory)) {
            $doneFile = $this->doneDirectory . $this->getFileNameFromPath($srcFile);
            rename($srcFile, $doneFile);
        } else {
            mkdir($this->doneDirectory, 0700);
        }
    }

    function getAVars($o) {
        $aVars = array();

        $aVars['whseArrivaleDate'] = $o->getWhseDate();
        $aVars['whseArrivaleTime'] = $o->getWhseTime();
        $aVars['shptRef'] = $o->getShptRef();
        $aVars['pcs_received'] = $o->getShptRef();
        $aVars['ctn_received'] = $o->getShptRef();
        $aVars['pallet_received'] = $o->getShptRef();

        return $aVars;
    }

    function mTreatYR2YR3YR4($file) {
        $header = new TracingHeaderClass();
        $lines = file($file);
        foreach ($lines as $lineNumber => $lineContent) {
            switch (substr($lineContent, 0, 1)) {
                case 'E':
                    $header->setFilePath($file);
                    $header->setLineToAnalyse($lineContent);
                    break;
                case 'D':
                    $detail = new TracingDetailClass($lineContent);
                    $header->arrayDetailPush($detail);
                    break;
            }
        }
        return $header;
    }

    function execSqlrUpdate($oBldInput) {
        $execSqlrUpdate = new ExecSqlUpdateClass('pgsqlConfig.yml');
        $aVars = $this->getAVars($oBldInput);
        print $execSqlrUpdate->getSqlrFromVars('updateCpLoading.sql', $aVars);
        $execSqlrUpdate->execSqlrFromVars('updateCpLoading.sql', $aVars);
//TODO        print $execSqlrUpdate->getSqlrFromVars('updateCpLoadingPoSku.sql', $aVars);
//        $execSqlrUpdate->execSqlrFromVars('updateCpLoading.sql', $aVars);
        $execSqlrUpdate->closeDbConnection();
    }

    function printBldInput($o) {
        $o->printAll();
        var_dump($o->getADetail());
    }

    function removeBackSlash($string) {
        return str_replace('\\', '', $string);
    }

    function getFileNameFromPath($string) {
        $pattern = "/.*\//";
        $replacement = '';
        $string = preg_replace($pattern, $replacement, $string);
        return $string;
    }
}
