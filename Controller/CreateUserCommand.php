<?php

// src/AppBundle/Command/CreateUserCommand.php

namespace AppBundle\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Finder\Finder;

class CreateUserCommand extends Command {

    protected function configure() {
        $this
// the name of the command (the part after "bin/console")
                ->setName('app:create-user')

// the short description shown while running "php bin/console list"
                ->setDescription('Creates a new user.')

// the full command description shown when running the command with
// the "--help" option
                ->setHelp('This command allows you to create a user...')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output) {
        $output->writeln([
            'User Creator',
            '============',
            '',
        ]);

        // outputs a message followed by a "\n"
        $output->writeln('Whoa!');

        // outputs a message without adding a "\n" at the end of the line

$finder = new Finder();
//$finder->files()->in(__DIR__);
$finder
->files()->in('/home/tmp/BLD_cp')
//->name('*YR2*')
//->contains('/^.{41}CP[0-9][0-9][0-9][0-9][0-9]\ /') // tous les fichiers qui contiennent 41 caractères puis un chaine commencant par 'CP' puis 5 chiffres puis espace 
->sortByName();

;


foreach ($finder as $file) {
	$fileContent = $file->getContents();
	if(strpos($file,'YR2')>0){
		$this->mTreatYR2($output, $fileContent);
	}
	if(strpos($file,'YR3')>0){
//		$this->mTreatYR2($output, $fileContent);
	}
	if(strpos($file,'YR4')>0){
//		$this->mTreatYR2($output, $fileContent);
	}
}

        $this->aMethod($output);
        $var = $this->aFunction();
		
        $output->writeln($var);
        $output->write('You are about to ');
        $output->write('create a user.');
    }

    function aFunction() {
        return 'This is a function';
    }

    function aMethod(OutputInterface $output) {
        $output->writeln('This is a method');
    }

	function mTreatYR2(OutputInterface $output, $fileContent){
        $output->writeln($fileContent);
	}
	
	function mTreatYR3(OutputInterface $output, $fileContent){
        $output->writeln($fileContent);
	}
	
	function mTreatYR4(OutputInterface $output, $fileContent){
        $output->writeln($fileContent);
	}
	
}
