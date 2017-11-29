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
    /*
     * La fonction suivante executera l'insertion dans la base de donnée
     */
    function execSqlRequest($num_po, $type, $evenement) {
        
        //$this->output->writeln($this->getInsertIntoEvenements_poSqlRequest($num_po, $type, $evenement));
        $sqlr = $this->getInsertIntoEvenements_poSqlRequest($num_po, $type, $evenement);

//        $execSqlrUpdate = new ExecSqlUpdateClass('pgsqlConfig.yml');
//        $execSqlrUpdate->execSqlr($sqlr);
//        $execSqlrUpdate->closeDbConnection();


        $connexion = pg_connect("host=localhost port=5432 dbname=yrocher user=postgres password=lp4U58c")
                or die('Connexion impossible : ' . pg_last_error());
        pg_query($connexion, $sqlr);
        pg_close($connexion);
    }
    function connexionBdd($sqlr) {
        
    }
    function getInsertIntoEvenements_poSqlRequest($num_po, $type, $evenement) {
        $sqlr = "INSERT INTO evenements_po (id_po, date_evenement, heure_evenement, login, type, evenement)
    VALUES (
    SUBSTR('" . $num_po . "', 1, 20), 
    TO_CHAR(NOW(), 'YYYYMMDD'),  
    TO_CHAR(NOW(), 'HH24MI'), 
    'import', 
    '" . $type . "', 
    '" . $evenement . "'
    );
    ";
        return $sqlr;
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
        
        /*
         * YR4 STRIPPED DATE Auto Stripped Date for container CP?????.
         * YR3 ARRIVED FND DATE Auto Arrived FND Date for container CP?????.
         * YR2 PLANNED DATE [YR1]Auto Planned Date for container CP?????.
         */
        
        $type = 'STRIPPED DATE';
        $evenement = 'Auto Stripped Date for container CP?????.';
        
//        $this->getFirstCahrOfString(3, $file);
        //get3 premier caratère du nom du fichier
        
        //if commence par YR2 
        $type = 'PLANNED DATE';
        $evenement = 'Auto Planned Date for container CP?????.';
                //if commence par YR3 
        $type = 'ARRIVED FND DATE';
        $evenement = 'Auto Arrived FND Date for container CP?????.';
        
//if commence par YR4 
        $type = 'STRIPPED DATE';
        $evenement = 'Auto Stripped Date for container CP?????.';
        
        $this->execSqlRequest($file,$type , $evenement);
        $oBldInput = $this->mTreatYR2YR3YR4($file);
        $shptRef = $oBldInput->getShptRef();
//        $this->printBldInput($oBldInput);
//        $this->execSqlrUpdateHeader($oBldInput);
//        $this->execSqlrUpdateDetail($oBldInput);
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
//        $this->output->writeln($execSqlrUpdate->getSqlrFromVars('updateHeader.sql', $headerVars));
        $execSqlrUpdate->execSqlrFromVars('updateHeader.sql', $headerVars);
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
			if($i == 0){
				
				$sqlr .= "SELECT cp.init_cp_loading_sku_received('".$detailVars['shptRef']."', '".$detailVars['numPoRoot']."', '".$detailVars['sku']."');";
				$i++;
			}
			$sqlr .= "SELECT cp.update_cp_loading_sku_received('".$detailVars['shptRef']."', '".$detailVars['numPoRoot']."', '".$detailVars['sku']."', ".$detailVars['ctn'].", ".$detailVars['pcs'].");";
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