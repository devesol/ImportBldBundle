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

// php bin/console treatFile


require 'src/FCS/ImportBldBundle/Command/TracingHeaderClass.php';

class TreatFileCommand extends ContainerAwareCommand {

    private $srcDirectory;
    private $doneDirectory;
    private $output;

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
        $this->output = $output;
        $this->init();
        $this->listenDirectory($output);
    }

    function init() {

        $this->output->writeln($this->getYmlPathFromClassPath());
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
            $output->writeln($file);
            $pattern = '/.*YR[2|3|4]_.+/';
            if (preg_match($pattern, $file, $matches, PREG_OFFSET_CAPTURE)) {
                $this->execForEachFile($file);
            }
        }
    }

    function execForEachFile($file) {
        $this->output->writeln($file);
        $oBldInput = $this->mTreatYR2YR3YR4($file);
//        $this->printBldInput($oBldInput);
        $this->execSqlrUpdateHeader($oBldInput);
        $this->execSqlrUpdateDetail($oBldInput);
//        $this->mvFileToDone($file);
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

    function getHeaderVars(TracingHeaderClass $o) {
        $a = array();
        $a['whseArrivaleDate'] = $o->getWhseDate();
        $a['whseArrivaleTime'] = $o->getWhseTime();
        $a['shptRef'] = $o->getShptRef();
        return $a;
    }

    function getDetailVars(TracingDetailClass $o) {
        $a = array();
        $a['numPoRoot'] = $o->getPoRoot();
        $a['sku'] = $o->getSku();
        $a['pcs'] = $o->getPcs();
        $a['ctn'] = $o->getCtn();
        $a['numPoste'] = $o->getNumPoste();
        $a['codePays'] = $o->getCodePays();
        $a['reseauBld'] = $o->getReseauBld();
        return $a;
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

    function execSqlrUpdateHeader(TracingHeaderClass $o) {
        $execSqlrUpdate = new ExecSqlUpdateClass('pgsqlConfig.yml');
        $headerVars = $this->getHeaderVars($o);
        $this->output->writeln($execSqlrUpdate->getSqlrFromVars('updateHeader.sql', $headerVars));
        $execSqlrUpdate->execSqlrFromVars('updateHeader.sql', $headerVars);
        $execSqlrUpdate->closeDbConnection();
    }

    function execSqlrUpdateDetail(TracingHeaderClass $o) {
        $this->output->writeln("execSqlrUpdateDetail");
        $execSqlrUpdate = new ExecSqlUpdateClass('pgsqlConfig.yml');
        foreach ($o->getADetail() as $tracingDetailObject) {
            $execSqlrUpdate = new ExecSqlUpdateClass('pgsqlConfig.yml');
            $detailVars = $this->getDetailVars($tracingDetailObject);
            $detailVars['shptRef'] = $o->getShptRef();
            $this->output->writeln($execSqlrUpdate->getSqlrFromVars('updateDetail.sql', $detailVars));
            $execSqlrUpdate->execSqlrFromVars('updateDetail.sql', $detailVars);
            $execSqlrUpdate->closeDbConnection();
        }
    }

    function printBldInput(TracingHeaderClass $o) {
        $o->printAll();
        var_dump($o->getADetail());
    }

    function removeBackSlash($string) {
        return str_replace('\\', '', $string);
    }

    function getFileNameFromPath($string) {
        $pattern = "/.*\//";
        $replacement = '  ';
        $string = preg_replace($pattern, $replacement, $string);
        return $string;
    }

}
