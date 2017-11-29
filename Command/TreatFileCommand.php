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
//require 'src/FCS/ImportBldBundle/Command/TracingHeaderClass.php';
class TreatFileCommand extends ContainerAwareCommand {

    private $srcDirectory;
    private $doneDirectory;
    private $logPath;
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
        $this->listenDirectory();
        //$this->connexionBdd();
    }

    function init() {
        $this->output->writeln(__DIR__ . '/' . $this->getYmlPathFromClassPath());
        $vars = new Vars(__DIR__ . '/' . $this->getYmlPathFromClassPath());

        $this->output->writeln($vars['parameters.srcDirectory']);

        $this->srcDirectory = $vars['parameters.srcDirectory'];
        $this->doneDirectory = $vars['parameters.doneDirectory'];
        $this->logPath = $vars['parameters.logPath'];
        print $this->logPath . "]]" . $this->doneDirectory;
    }

    function getYmlPathFromClassPath() {
        $paramFilePath = __CLASS__;
        $pattern = "/.*\\\/";
        $replacement = '';
        $paramFilePath = preg_replace($pattern, $replacement, $paramFilePath);
        return $paramFilePath . ".yml";
    }

    function listenDirectory() {
        $finder = new Finder();
        $finder
                ->files()->in($this->srcDirectory)
                ->sortByName()
                ->depth('== 0');
        foreach ($finder as $file) {
            $pattern = '/.*YR[2|3|4]_.+/';
            if (preg_match($pattern, $file, $matches, PREG_OFFSET_CAPTURE)) {
                $this->execForEachFile($file);
            } else {
                $this->addLog("Erreur de fichier", $file); // Ne fonctionne pas !! 
                $this->output->writeln("On ne traite pas le fichier " . $file);
            }
        }
    }

    function execForEachFile($file) {
        $this->addLog("DEBUT TRAITEMENT ", $file);

        $oBldInput = $this->mTreatYR2YR3YR4($file);
        $shptRef = $oBldInput->getShptRef();
        $this->addLog("FIN TRAITEMENT ", $file);
        $this->output->writeln("Traitement du fichier " . $file);
        $oBldInput = $this->mTreatYR2YR3YR4($file);
//        $this->printBldInput($oBldInput);
        $posCpInShptRef = stripos($oBldInput->getShptRef(), 'CP');
        $this->output->writeln("stripos" . $posCpInShptRef);
        if ($posCpInShptRef == 0 && $posCpInShptRef !== false) {
            $this->output->writeln(" on traite le fichier car le shpt Ref commence par CP");
            $this->execSqlrUpdateHeader($oBldInput);
            $this->execSqlrUpdateDetail($oBldInput);
            $this->mvFileToDone($file);
            $this->output->writeln("Fin de traitement du fichier " . $file);
        } else {
            $this->output->writeln(" on ne traite pas le fichier car le shpt Ref ne commence pas par CP");
        }
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
        $a['filePrefix'] = $o->getFilePrefix();
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
        $sqlr = "UPDATE cp_loading SET delivery_real_date = '" . $headerVars['whseArrivaleDate'] . "', delivery_real_time = '" . $headerVars['whseArrivaleTime'] . "' WHERE ref =  TRIM('" . $headerVars['shptRef'] . "');";
        $execSqlrUpdate->execSqlr($sqlr);
//        $this->setEvenementsPo($o);
        $sqlr = "SELECT cp.bld_import_insert_into_evenements_po('" . $headerVars['filePrefix'] . "', '" . $headerVars['shptRef'] . "');";
        $this->output->writeln($sqlr);
        $execSqlrUpdate->execSqlr($sqlr);
        $execSqlrUpdate->closeDbConnection();
    }

    function execSqlrUpdateDetail(TracingHeaderClass $o) {
        $this->output->writeln("execSqlrUpdateDetail");
        $execSqlrUpdate = new ExecSqlUpdateClass('pgsqlConfig.yml');
        $i = 0;
        foreach ($o->getADetail() as $tracingDetailObject) {
            $detailVars = $this->getDetailVars($tracingDetailObject);
            $detailVars['shptRef'] = $o->getShptRef();
//            $this->output->writeln($execSqlrUpdate->getSqlrFromVars('updateDetail.sql', $detailVars));
            $sqlr = "";
            if ($i == 0) {

                $sqlr .= "SELECT cp.init_cp_loading_sku_received('" . $detailVars['shptRef'] . "', '" . $detailVars['numPoRoot'] . "', '" . $detailVars['sku'] . "');";
                $sqlr .= "\n";
                $i++;
            }
            $sqlr .= "SELECT cp.update_cp_loading_sku_received('" . $detailVars['shptRef'] . "', '" . $detailVars['numPoRoot'] . "', '" . $detailVars['sku'] . "', " . $detailVars['ctn'] . ", " . $detailVars['pcs'] . ");";
            $this->output->writeln($sqlr);
            $execSqlrUpdate->execSqlr($sqlr);
        }

        $execSqlrUpdate->closeDbConnection();
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
        $replacement = '';
        $string = preg_replace($pattern, $replacement, $string);
        return $string;
    }

    function addLog($text, $file) {
        $text = $text . $this->getFileNameFromPath($file) . " " . date("[j/m/y H:i:s]");
        file_put_contents($this->logPath, $text . "\r\n", FILE_APPEND);
        // EXEMPLE : DEBUT TRAITEMENT YR2_2309157918 2015-09-23 11:50:01
    }

}
