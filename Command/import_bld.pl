#!/usr/bin/perl
#===========================================================================
# Historique    :
# Le 22 Février 2010
#	Mise à jour des qtés stripped du flux YR4
# Le 15 Mars 2010
#	Intégration du flux YR5
# JUIN 2010
#	Ajout alimentation tables delivery en réception des FLUX YR4
#	Seuls les flux tirés seront utilisés dans la fonction Final Delivery
# 6 JUILLET 2010 : Envoi des BL au format PDF vers YRGROUP
# Le 18 Avril 2011 :
#	Modification des dates contenues dans le flux YR5
# Le 8 Décembre 2011
#	Modification flux YR1, on reçoit le numéro de container maintenant dans le flux
# Le 15 Février 2012 Modification traitement Flux YR1
# 	Pour rechercher les CONTAINERS concernés on prend l'année et le mois de etd OU eta à la place de etd ET eta
# Le 7 Septembre 2012
# 	Ajout pris en compte de l'heure VAQ dans le flux YR1
#===========================================================================


use strict;

use DBI;
use Class::Date qw(:errors date now);
use File::Copy;
use IO::File;
use MIME::Lite;

use lib "/home/fcs/clients/yrocher/web/params/";    # permet d'ajouter des repertoires à @INC
use lib "/home/fcs/clients/yrocher/web/lib/";       # permet d'ajouter des repertoires à @INC
use fcs_conf_yrocher;
use fcs_lib_yrocher;

use lib "/home/fcs/clients/";
use fcs_lib;
use fcs_fonctions_globales;

my $directory_in = "/home/fcstramar/";
if ( $fcs_conf_yrocher::mode_debug ) { $directory_in = $fcs_home_dir . "/bin" };
my $directory_save = $fcs_home_dir . "/bin/save_import_bld/";
my $fichier_log = $fcs_home_dir . "/bin/import_bld.log";
my $fichier_lock = $fcs_home_dir . "/bin/import_bld.lck";
open( FICHIER_LOG, ">> $fichier_log" ) || die "Impossible ouvrir fichier log !\n";
my $tmp_date = now;
my $file_log_ftp = "/var/log/xferlog";
my $login_bld = "fcstramar";

print FICHIER_LOG "\n\nDEBUT TRAITEMENT du $tmp_date";

# On regarde s'il y a déjà un traitement en cours
if(-f $fichier_lock) {
  print FICHIER_LOG "Traitement en cours \n";
  close FICHIER_LOG;
  &send_mail_alerte();
  exit;
}
# On crée un fichier de lock
open(FICHIER_LOCK, "> $fichier_lock");
print FICHIER_LOCK  "EN COURS\n";
close(FICHIER_LOCK);

# Tentative de connexion à la base:
my $dbh = DBI->connect( $base_fcs_dsn, $base_fcs_user, $base_fcs_password, { AutoCommit => 1 } );
if ( $dbh eq undef ) {
  print FICHIER_LOG "\nERROR Connexion base.";
  close FICHIER_LOG;
  close FICHIER_LOCK;
  unlink $fichier_lock;
  exit;
}

# On se positionne dans le répertoire qui reçoit les fichiers 
chdir $directory_in;

# TRAITEMENT DES FICHIERS YR1_* 
# =============================
my $sqlr = " SELECT
		TRIM(container) as container,
		TRIM(num_po) as num_po,
		TRIM(linerterm) as linerterm
		FROM tracing_container_po_mer
		WHERE code_transitaire = ?
		AND pol = ?
		AND pod = ?
		AND ( substr(etd,1,6) = substr(?,1,6) OR substr(eta,1,6) = substr(?,1,6) )
AND CONTAINER = ?
		AND ( statut = 'S' OR statut = 'P' )
";
my $rs = $dbh->prepare( $sqlr );

my $uqlr = " UPDATE tracing_container_po_mer
		SET arrival_pod = ?,
		arrival_pod_time = ?,
		fnd_proposed_date = ?,
		statut = 'D'
		WHERE container = ?
		AND num_po = ?
		AND ( statut = 'S' OR statut = 'P' )
";
my $ru = $dbh->prepare( $uqlr );

open(PIPE_LS, "/bin/ls YR1_* 2>/dev/null |");
while(<PIPE_LS>) {
  my $file_in = $_;
  $file_in =~ s/\n$//;
  $file_in =~ s/\r$//;

  # ON REGARDE SI LE TRANSFERT EST TERMINE ( ANALYSE DU FICHIER xferlog )
  my $transfert_OK = 0;
  open(FTP_LOG, "+< $file_log_ftp");
  while(<FTP_LOG>) {
    my $xtemp = $_;
    $xtemp =~ s/\n$//;
    $xtemp =~ s/\r$//;
    my @res_split = split(/\s+/, $xtemp); # $res_split[13] contient le login
    if ( $res_split[13] ne $login_bld ) { 
      next; 
    }
    if ( "/" . $file_in eq $res_split[8] and $res_split[17] eq "c" ) { # le Transfert est terminé
      $transfert_OK = 1;
      last;
    }
  }
  close(FTP_LOG);

  if ( $fcs_conf_yrocher::mode_debug ) { $transfert_OK = 1; }

  if ( $transfert_OK ) {
	print FICHIER_LOG "Début du traitement du fichier $file_in \n";
    open(FILE_IN, $file_in);
    while(<FILE_IN>) {
      my $ligne = $_;
      my $fwd = &trimwhitespace(substr($ligne, 41, 35 ));
      my $pol = &trimwhitespace(substr($ligne, 76, 5 ));
      my $pod = &trimwhitespace(substr($ligne, 81, 5 ));
      my $etd = &trimwhitespace(substr($ligne, 86, 8 ));
      my $eta = &trimwhitespace(substr($ligne, 94, 8 ));
      my $arr_date = &trimwhitespace(substr($ligne, 102, 8 ));
      my $arr_time = &trimwhitespace(substr($ligne, 125, 4 ));
my $ct = &trimwhitespace(substr($ligne, 110, 15 ));
      print FICHIER_LOG "\nTraitement $file_in : FWD=$fwd, POL=$pol, POD=$pod, ETD=$etd, ETA=$eta, CT=$ct, ARR=$arr_date,$arr_time";
      if ( substr($arr_date, 0, 1) eq '2' ) { # la date semble renseignée
        $rs->execute( $fwd, $pol, $pod, $etd, $eta, $ct );
	if ( $dbh->errstr ne undef ) {
	  print FICHIER_LOG $dbh->errstr."\n".$sqlr;
	  $rs->finish;
	  $dbh->disconnect;
	  close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
	}
	while( my $data = $rs->fetchrow_hashref ) {
          my $date_tmp = date { year => substr( $arr_date, 0, 4 ), month => substr( $arr_date, 4, 2 ), day => substr( $arr_date, 6, 2 ) };
	  if ( $data->{'linerterm'} eq 'LCL' ) {
	    $date_tmp = $date_tmp + '7D';
	  } else {
	    $date_tmp = $date_tmp + '2D';
	  }
	  my $date_planned = $date_tmp->year . sprintf( "%02d", $date_tmp->month ) . sprintf( "%02d", $date_tmp->day );
          $ru->execute( $arr_date, $arr_time, $date_planned, $data->{'container'}, $data->{'num_po'} );
	  if ( $dbh->errstr ne undef ) {
    	    print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	    $ru->finish;
	    $rs->finish;
	    $dbh->disconnect;
	    close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
	  }
&FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "PLANNED DATE", '[YR1]Auto Planned Date for container ' . $data->{'container'} . '.', 'import' );

	  print FICHIER_LOG "\n  ==>  MAJ PLANNED=$date_planned pour CT=$data->{'container'}, PO=$data->{'num_po'}, LINERTERM=$data->{'linerterm'}, ARRIVED POD=$arr_date";
	}
	$rs->finish;
      }
    }
    close(FILE_IN);
    move($file_in, $directory_save.$file_in);
  }
}
close(PIPE_LS);

# TRAITEMENT DES FICHIERS YR2_* 
# =============================
$sqlr = " SELECT
		TRIM(container) as container,
		TRIM(num_po) as num_po,
		statut as statut,
		TRIM(arrival_pod) as arrival_pod,
	        TRIM(linerterm) as linerterm
		FROM tracing_container_po_mer
		WHERE SUBSTR(container,1,11) = ?
		AND ( statut = 'S' OR statut = 'P' OR statut = 'D' )
";
$rs = $dbh->prepare( $sqlr );

$uqlr = " UPDATE tracing_container_po_mer
		SET fnd_proposed_date = ?,
		fnd_proposed_time = ?,
                arrival_pod = ?,
                statut = 'D'
		WHERE SUBSTR(container,1,11) = ?
		AND num_po = ?
		AND ( statut = 'S' OR statut = 'P' OR statut = 'D' )
";
$ru = $dbh->prepare( $uqlr );

open(PIPE_LS, "/bin/ls YR2_* 2>/dev/null |");
while(<PIPE_LS>) {
  my $file_in = $_;
  $file_in =~ s/\n$//;
  $file_in =~ s/\r$//;

  # ON REGARDE SI LE TRANSFERT EST TERMINE ( ANALYSE DU FICHIER xferlog )
  my $transfert_OK = 0;
  open(FTP_LOG, "+< $file_log_ftp");
  while(<FTP_LOG>) {
    my $xtemp = $_;
    $xtemp =~ s/\n$//;
    $xtemp =~ s/\r$//;
    my @res_split = split(/\s+/, $xtemp); # $res_split[13] contient le login
    if ( $res_split[13] ne $login_bld ) { 
      next; 
    }
    if ( "/" . $file_in eq $res_split[8] and $res_split[17] eq "c" ) { # le Transfert est terminé
      $transfert_OK = 1;
      last;
    }
  }
  close(FTP_LOG);

  if ( $fcs_conf_yrocher::mode_debug ) { $transfert_OK = 1; }

  if ( $transfert_OK ) {
	print FICHIER_LOG "Début du traitement du fichier $file_in \n";
    open(FILE_IN, $file_in);
    while(<FILE_IN>) {
      my $ligne = $_;
      my $ct = &trimwhitespace(substr($ligne, 41, 11 ));
      my $whse_date = &trimwhitespace(substr($ligne, 56, 8 ));
      my $whse_time = &trimwhitespace(substr($ligne, 64, 4 ));
      print FICHIER_LOG "\nTraitement $file_in : CT=$ct, DateRDV=$whse_date, Time_RDV=$whse_time";      
      if ( substr($whse_date, 0, 1) eq '2' ) { # la date semble renseignée
        $rs->execute( $ct );
	if ( $dbh->errstr ne undef ) {
	  print FICHIER_LOG $dbh->errstr."\n".$sqlr;
	  $rs->finish;
	  $dbh->disconnect;
	  close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
	}
	while( my $data = $rs->fetchrow_hashref ) {
          # SI ON EST PLANNED 
          if ( $data->{'statut'} eq 'D' ) { 
            $ru->execute( $whse_date, $whse_time, $data->{'arrival_pod'}, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
            &FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "PLANNED DATE", '[YR2]Auto Planned Date for container ' . $data->{'container'} . '.', 'import' );
	    print FICHIER_LOG "\n  ==> MAJ PLANNED=$whse_date,$whse_time pour  CT=$data->{'container'}, PO=$data->{'num_po'}";
          }

          # SI ON EST ARRIVED POD
          if ( $data->{'statut'} eq 'P' ) { 
            $ru->execute( $whse_date, $whse_time, $data->{'arrival_pod'}, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
            &FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "PLANNED DATE", '[YR2]Auto Planned Date for container ' . $data->{'container'} . '(ARRIVED POD => PLANNED).', 'import' );
	    print FICHIER_LOG "\n  ==> MAJ PLANNED=$whse_date,$whse_time pour  CT=$data->{'container'}, PO=$data->{'num_po'}, (ARRIVED POD => PLANNED)";
          }

          # SI ON EST SAILING 
          if ( $data->{'statut'} eq 'S' ) { 
            my $date_tmp = date { year => substr( $whse_date, 0, 4 ), month => substr( $whse_date, 4, 2 ), day => substr( $whse_date, 6, 2 ) };
            if ( $data->{'linerterm'} eq 'LCL' ) {
              $date_tmp = $date_tmp - '7D';
            } else {
              $date_tmp = $date_tmp - '2D';
            }
	    my $date_arrived = $date_tmp->year . sprintf( "%02d", $date_tmp->month ) . sprintf( "%02d", $date_tmp->day );
            
            $ru->execute( $whse_date, $whse_time, $date_arrived, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
            &FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "PLANNED DATE", 'Auto Planned Date for container ' . $data->{'container'} . '(SAILING => PLANNED).', 'import' );
	    print FICHIER_LOG "\n  ==> MAJ PLANNED=$whse_date,$whse_time pour  CT=$data->{'container'}, PO=$data->{'num_po'}, (SAILING => PLANNED)";
          }
	}
	$rs->finish;
      }

    }
    close(FILE_IN);
    move($file_in, $directory_save.$file_in);
  }
}
close(PIPE_LS);

# TRAITEMENT DES FICHIERS YR3_* 
# =============================
$sqlr = " SELECT
                TRIM(container) as container,
                TRIM(num_po) as num_po,
                statut as statut,
		TRIM(fnd_proposed_date) as planned_date,
		TRIM(fnd_proposed_time) as planned_time,
                TRIM(arrival_pod) as arrival_pod,
                TRIM(linerterm) as linerterm
                FROM tracing_container_po_mer
                WHERE SUBSTR(container,1,11) = ?
                AND ( statut = 'S' OR statut = 'P' OR statut = 'D' )
";

$rs = $dbh->prepare( $sqlr );

$uqlr = " UPDATE tracing_container_po_mer
		SET fnd_confirmed_date = ?,
                fnd_confirmed_time = ?,
		fnd_proposed_date = ?,
		fnd_proposed_time = ?,
                arrival_pod = ?,
		statut = 'R'
		WHERE SUBSTR(container,1,11) = ?
		AND num_po = ?
		AND ( statut = 'S' OR statut = 'P' OR statut = 'D' )
";
$ru = $dbh->prepare( $uqlr );

open(PIPE_LS, "/bin/ls YR3_* 2>/dev/null |");
while(<PIPE_LS>) {
  my $file_in = $_;
  $file_in =~ s/\n$//;
  $file_in =~ s/\r$//;
  # ON REGARDE SI LE TRANSFERT EST TERMINE ( ANALYSE DU FICHIER xferlog )
  my $transfert_OK = 0;
  open(FTP_LOG, "+< $file_log_ftp");
  while(<FTP_LOG>) {
    my $xtemp = $_;
    $xtemp =~ s/\n$//;
    $xtemp =~ s/\r$//;
    my @res_split = split(/\s+/, $xtemp); # $res_split[13] contient le login
    if ( $res_split[13] ne $login_bld ) { 
      next; 
    }
    if ( "/" . $file_in eq $res_split[8] and $res_split[17] eq "c" ) { # le Transfert est terminé
      $transfert_OK = 1;
      last;
    }
  }
  close(FTP_LOG);

  if ( $fcs_conf_yrocher::mode_debug ) { $transfert_OK = 1; }

  if ( $transfert_OK ) {
	print FICHIER_LOG "Début du traitement du fichier $file_in \n";
    open(FILE_IN, $file_in);
    while(<FILE_IN>) {
      my $ligne = $_;
      my $ct = &trimwhitespace(substr($ligne, 41, 11 ));
      my $whse_date = &trimwhitespace(substr($ligne, 56, 8 ));
      my $whse_time = &trimwhitespace(substr($ligne, 64, 4 ));
      print FICHIER_LOG "\nTraitement $file_in : CT=$ct, Date_FND=$whse_date, Time_FND=$whse_time";      
      if ( substr($whse_date, 0, 1) eq '2' ) { # la date semble renseignée
        $rs->execute( $ct );
	if ( $dbh->errstr ne undef ) {
	  print FICHIER_LOG $dbh->errstr."\n".$sqlr;
	  $rs->finish;
	  $dbh->disconnect;
	  close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
	}
	while( my $data = $rs->fetchrow_hashref ) {
          # SI ON EST PLANNED
          if ( $data->{'statut'} eq 'D' ) { 
            $ru->execute( $whse_date, $whse_time, $data->{'planned_date'}, $data->{'planned_time'}, $data->{'arrival_pod'}, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
 	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
            &FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "ARRIVED FND DATE", 'Auto Arrived FND Date for container ' . $data->{'container'} . '.', 'import' );
	    print FICHIER_LOG "\n  ==> MAJ ARRIVED=$whse_date,$whse_time pour CT=$data->{'container'}, PO=$data->{'num_po'}";
          }

          # SI ON EST ARRIVED POD
          if ( $data->{'statut'} eq 'P' ) { 
            $ru->execute( $whse_date, $whse_time, $whse_date, $whse_time, $data->{'arrival_pod'}, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
 	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
            &FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "ARRIVED FND DATE", 'Auto Arrived FND Date for container ' . $data->{'container'} . '(ARRIVED POD => ARRIVED FND).', 'import' );
	    print FICHIER_LOG "\n  ==> MAJ ARRIVED=$whse_date,$whse_time pour CT=$data->{'container'}, PO=$data->{'num_po'}, (ARRIVED POD => ARRIVED FND)";
          }

          # SI ON EST SAILING 
          if ( $data->{'statut'} eq 'S' ) { 
            my $date_tmp = date { year => substr( $whse_date, 0, 4 ), month => substr( $whse_date, 4, 2 ), day => substr( $whse_date, 6, 2 ) };
            if ( $data->{'linerterm'} eq 'LCL' ) {
              $date_tmp = $date_tmp - '7D';
            } else {
              $date_tmp = $date_tmp - '2D';
            }
	    my $date_arrived = $date_tmp->year . sprintf( "%02d", $date_tmp->month ) . sprintf( "%02d", $date_tmp->day );

            $ru->execute( $whse_date, $whse_time, $whse_date, $whse_time, $date_arrived, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
 	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
            &FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "ARRIVED FND DATE", 'Auto Arrived FND Date for container ' . $data->{'container'} . '(SAILING => ARRIVED FND).', 'import' );
	    print FICHIER_LOG "\n  ==> MAJ ARRIVED=$whse_date,$whse_time pour CT=$data->{'container'}, PO=$data->{'num_po'}, (SAILING => ARRIVED FND)";
          }
	}

	$rs->finish;
      }

    }
    close(FILE_IN);
    move($file_in, $directory_save.$file_in);
  }
}
close(PIPE_LS);

# TRAITEMENT DES FICHIERS YR4_* 
# =============================
$sqlr = " SELECT
		TRIM(container) as container,
		TRIM(num_po) as num_po
		FROM tracing_container_po_mer
		WHERE SUBSTR(container,1,11) = ?
		AND statut = 'R'
";
$rs = $dbh->prepare( $sqlr );

$uqlr = " UPDATE tracing_container_po_mer
		SET fnd_arrival_date = ?,
		fnd_arrival_time = ?,
		statut = 'F'
		WHERE SUBSTR(container,1,11) = ?
		AND num_po = ?
		AND statut = 'R'
";
$ru = $dbh->prepare( $uqlr );

my $sdel = " DELETE FROM stripped_tmp";
my $rdel = $dbh->prepare( $sdel );
$rdel->execute();
if ( $dbh->errstr ne undef ) {
  print FICHIER_LOG $dbh->errstr."\n".$sdel;
  $rdel->finish;
  $dbh->disconnect;
  close FICHIER_LOG;
  unlink $fichier_lock;
  exit;
}
$rdel->finish;
my $stmp = " SELECT pcs, ctn 
	     FROM stripped_tmp
	     WHERE container = ?
	     AND po_root = ?
	     AND fnd_initial_date = ?
	     AND sku_id = ?
";
my $rtmp = $dbh->prepare( $stmp );
my $sitmp = " INSERT INTO stripped_tmp (container, po_root, fnd_initial_date, sku_id, pcs, ctn)
	      VALUES ( ?, ?, ?, ?, ?, ? )
";
my $ritmp = $dbh->prepare( $sitmp );
my $sutmp = " UPDATE stripped_tmp
	      SET pcs = ?, ctn = ?
	      WHERE container = ?
	      AND po_root = ?
	      AND fnd_initial_date = ?
	      AND sku_id = ?
";
my $rutmp = $dbh->prepare( $sutmp );

# JUIN 2010
#	Ajout alimentation tables delivery en réception des FLUX YR4
#	le num_do contient dans ce cas le numéro du CONTAINER
####################################################################
my $sdh = " SELECT *
	FROM delivery_header
	WHERE num_do = ? 
	AND ( TRIM(nom_livraison) = '' OR nom_livraison IS NULL )
";
my $rdh = $dbh->prepare( $sdh );

my $sidh = " INSERT INTO delivery_header ( num_do, date_depart, heure_depart, in_road_tracing )
		VALUES ( ?, ?, ?, '1')
";
my $ridh = $dbh->prepare( $sidh );

my $sdd = " SELECT *
	FROM delivery_detail d
	LEFT JOIN delivery_header h
	ON h.id = d.id_header
	WHERE TRIM(h.num_do) = ?
	AND ( TRIM(h.nom_livraison) = '' OR h.nom_livraison IS NULL )
	AND TRIM(d.num_bl) = ? 
	AND TRIM(d.num_po_root) = ?
	AND TRIM(d.num_sku) = ?
	AND TRIM(d.pays_bld) = ?
	AND TRIM(d.reseau_bld) = ?
";
my $rdd = $dbh->prepare( $sdd );

my $sidd = " INSERT INTO delivery_detail 
( id_header, num_bl, num_po_root, num_sku, pays_bld, reseau_bld, cpc_fcs, pieces, colis )
VALUES( 
( SELECT id FROM delivery_header WHERE num_do = ? AND ( TRIM(nom_livraison) = '' OR nom_livraison IS NULL ) ), 
?, ?, ?, ?, ?, ?, ?, ? )
";
my $ridd = $dbh->prepare( $sidd );
# Fin JUIN 2010
###############

open(PIPE_LS, "/bin/ls YR4_* 2>/dev/null |");
while(<PIPE_LS>) {
  my $file_in = $_;
  $file_in =~ s/\n$//;
  $file_in =~ s/\r$//;

  # ON REGARDE SI LE TRANSFERT EST TERMINE ( ANALYSE DU FICHIER xferlog )
  my $transfert_OK = 0;
  open(FTP_LOG, "+< $file_log_ftp");
  while(<FTP_LOG>) {
    my $xtemp = $_;
    $xtemp =~ s/\n$//;
    $xtemp =~ s/\r$//;
    my @res_split = split(/\s+/, $xtemp); # $res_split[13] contient le login
    if ( $res_split[13] ne $login_bld ) { 
      next; 
    }
    if ( "/" . $file_in eq $res_split[8] and $res_split[17] eq "c" ) { # le Transfert est terminé
      $transfert_OK = 1;
      last;
    }
  }
  close(FTP_LOG);

  if ( $fcs_conf_yrocher::mode_debug ) { $transfert_OK = 1; }

  if ( $transfert_OK ) {
	print FICHIER_LOG "Début du traitement du fichier $file_in \n";
    my $ct = '';
    my $bl = '';
    open(FILE_IN, $file_in);
    while(<FILE_IN>) {
      my $ligne = $_;

      if ( substr($ligne, 0, 1 ) eq 'E' ) {
        $ct = &trimwhitespace(substr($ligne, 41, 11 ));
        my $whse_date = &trimwhitespace(substr($ligne, 56, 8 ));
        my $whse_time = &trimwhitespace(substr($ligne, 64, 4 ));
        print FICHIER_LOG "\nTraitement $file_in : CT=$ct, DateFND=$whse_date, TimeFND=$whse_time";      
        if ( substr($whse_date, 0, 1) eq '2' ) { # la date semble renseignée
          $rs->execute( $ct );
    	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sqlr;
	    $rs->finish;
	    $dbh->disconnect;
	    close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
	  }
	  while( my $data = $rs->fetchrow_hashref ) {
            $ru->execute( $whse_date, $whse_time, substr($data->{'container'},0,11), $data->{'num_po'} );
	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$uqlr;
	      $ru->finish;
	      $rs->finish;
	      $dbh->disconnect;
	      close FICHIER_LOG;
              unlink $fichier_lock;
              exit;
	    }
&FCS_AjouteEvenementPOBatch( $dbh, $data->{'num_po'}, "STRIPPED DATE", 'Auto Stripped Date for container ' . $data->{'container'} . '.', 'import' );
	    print FICHIER_LOG "\n  ==> $ct : MAJ STRIPPED=$whse_date,$whse_time pour CT=$data->{'container'}, PO=$data->{'num_po'}.";
	  }
	$rs->finish;
        }
  	# JUIN 2010
        #		Ajout alimentation tables delivery en réception des FLUX YR4
        #		le num_do contient dans ce cas le numéro du CONTAINER
        ######################################################################
	# ON ALIMENTE LE HEADER
        $rdh->execute( $ct );
    	if ( $dbh->errstr ne undef ) {
	  print FICHIER_LOG $dbh->errstr."\n".$sdh;
	  $rdh->finish;
	  $dbh->disconnect;
	  close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
	}
	if ( $rdh->rows == 0 ) {
	  $ridh->execute(  $ct, $whse_date, $whse_time );  
    	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sidh;
	    $ridh->finish;
	    $rdh->finish;
	    $dbh->disconnect;
	    close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
	  }
	  $ridh->finish;
	  print FICHIER_LOG "\n  ==>>>>> INSERTION delivery_header=$ct,$whse_date,$whse_time";
        }
	$rdh->finish;
  	# FIN JUIN 2010
      }
      
      if ( substr($ligne, 0, 1 ) eq 'D' ) {
        my $id_sku = &trimwhitespace(substr($ligne, 11, 15 ));
        my $po_root = &trimwhitespace(substr($ligne, 26, 10 ));
        my $ctn = &trimwhitespace(substr($ligne, 82, 7 ));
        my $pcs = &trimwhitespace(substr($ligne, 89, 10 ));
        my $fnd_initial_date = '20' . substr($ligne, 164, 2) . substr($ligne, 161, 2) . substr($ligne, 158, 2);

        $rtmp->execute( $ct, $po_root, $fnd_initial_date, $id_sku );
	if ( $dbh->errstr ne undef ) {
	  print FICHIER_LOG $dbh->errstr."\n".$stmp;
	  $rtmp->finish;
	  $dbh->disconnect;
	  close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
	}
        if ( $rtmp->rows > 0 ) {
          my @data = $rtmp->fetchrow; 
	  $rutmp->execute( $pcs + $data[0], $ctn + $data[1], $ct, $po_root, $fnd_initial_date, $id_sku );
	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sutmp;
	    $rutmp->finish;
	    $rtmp->finish;
	    $dbh->disconnect;
	    close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
	  }
	  $rutmp->finish;
	} else {
	  $ritmp->execute( $ct, $po_root, $fnd_initial_date, $id_sku, $pcs, $ctn );
	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sitmp;
	    $ritmp->finish;
	    $rtmp->finish;
	    $dbh->disconnect;
	    close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
          }
	  $ritmp->finish;
	}
	$rtmp->finish;

  	# JUIN 2010
        #		Ajout alimentation tables delivery en réception des FLUX YR4
        #		le num_do contient dans ce cas le numéro du CONTAINER
        ######################################################################
	# ON ALIMENTE LE DETAIL
        my $pays_bld = &trimwhitespace(substr($ligne, 74, 2 ));
	my $reseau_bld = &trimwhitespace(substr($ligne, 99, 3 ));

        my $code_wms = $pays_bld.$reseau_bld;
        # ON RECHERCHE LE CPC 
        my $scpc = "SELECT TRIM(code_entrepot) FROM ref_entrepot WHERE code_wms = '$code_wms' ";
	my $rcpc = $dbh->prepare( $scpc );
        $rcpc->execute( );
        if ( $dbh->errstr ne undef ) {
          print FICHIER_LOG $dbh->errstr."\n".$scpc;
          $rcpc->finish;
          $dbh->disconnect;
          close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
        }
	my $cpc = $rcpc->fetchrow;
        $rcpc->finish;
 
        # ON RECHERCHE LE BL
        my $sbl = "
	SELECT TRIM(tcpm.bill_of_lading)
	FROM floating_split_fnd fsf
	LEFT JOIN floating_split_sku fss
	ON fss.id = fsf.id_sku
	LEFT JOIN floating_split_bl fsb
	ON fsb.id = fss.id_bl
	LEFT JOIN tracing_container_po_mer tcpm
	ON tcpm.bill_of_lading = fsb.bl
  	WHERE fss.sku_id = '$id_sku'
	AND fsf.fnd = '$cpc'
	AND UPPER(SUBSTR(fss.po,1,10)) = '$po_root'
        AND TRIM(SUBSTR(tcpm.container,1,11)) = '$ct'
        ";
	my $rbl = $dbh->prepare( $sbl );
        $rbl->execute();
        if ( $dbh->errstr ne undef ) {
          print FICHIER_LOG $dbh->errstr."\n".$sbl;
          $rbl->finish;
          $dbh->disconnect;
          close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
        }
        my $bl =$rbl->fetchrow;
        $rbl->finish;

        $rdd->execute( $ct, $bl, $po_root, $id_sku, $pays_bld, $reseau_bld );
        if ( $dbh->errstr ne undef ) {
          print FICHIER_LOG $dbh->errstr."\n".$sdd;
          $rdd->finish;
          $dbh->disconnect;
          close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
        }
        if ( $rdd->rows == 0 && $cpc ne '' && $bl ne ''  ) {
          $ridd->execute( $ct, $bl, $po_root, $id_sku, $pays_bld, $reseau_bld, $cpc, $pcs, $ctn );
          if ( $dbh->errstr ne undef ) {
            print FICHIER_LOG $dbh->errstr."\n".$sidd;
            $ridd->finish;
            $rdd->finish;
            $dbh->disconnect;
            close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
          }
          $ridd->finish;
	  print FICHIER_LOG "\n  =====>>>>> INSERTION delivery_detail=$ct,$bl,$po_root,$id_sku,$pays_bld,$reseau_bld,$cpc,$pcs,$ctn";
        }
        $rdd->finish;
        # Fin JUIN 2010
        ###############
      }
    }
    close(FILE_IN);
    move($file_in, $directory_save.$file_in);
  }
}
close(PIPE_LS);
# ICI on traite le fichier intermédiaire du flux YR4
# ==================================================
# On regarde s'il existe un seul enregistrement dans tracing_container_detail_mer pour le clé CT + PO_ROOT + FND_INITIAL_DATE + SKU.
# Si plus d'un enregistrement sur la clé, on ne fait pas la mise à jour.
# Et on regarde si dans tracing_container_po_mer les qtés stripped ont été saisies.
# Si les quantités strippées sont renseignées, on ne fait pas la mise à jour.
# On ne fait la mise à jour que si le statut est F = STRIPPED 
# ==========================================================================
$stmp = " SELECT 
          TRIM(container),
	  TRIM(po_root),
	  TRIM(fnd_initial_date),
	  TRIM(sku_id),
	  pcs,
	  ctn 
	  FROM stripped_tmp
"; 
$rtmp = $dbh->prepare( $stmp );
$sqlr = " SELECT TRIM(td.num_po), 
          CASE 
	    WHEN td.stripped_sku IS NOT NULL THEN td.stripped_sku 
            ELSE 0
	  END AS stripped_sku
          FROM tracing_container_detail_mer td
	  LEFT JOIN tracing_container_po_mer tp
	  ON tp.container = td.container
	  AND tp.num_po = td.num_po
          AND tp.container = td.container
          WHERE TRIM(SUBSTR(td.container,1,11)) = ?
	  AND UPPER(substr(td.num_po,1,10)) = ?
	  AND TRIM(tp.fnd_initial_date) = ?
	  AND TRIM(td.sku_id) = ?
	  AND statut = 'F'
";
my $rqlr = $dbh->prepare( $sqlr );
my $sulr = " UPDATE tracing_container_detail_mer
	  SET stripped_sku = ?,
	  stripped_ctn = ?
	  WHERE TRIM(SUBSTR(container,1,11)) = ?
	  AND TRIM(num_po) = ?
	  AND TRIM(sku_id) = ?
";
my $rulr = $dbh->prepare( $sulr );

$rtmp->execute();
if ( $dbh->errstr ne undef ) {
  print FICHIER_LOG $dbh->errstr."\n".$stmp;
  $rtmp->finish;
  $dbh->disconnect;
  close FICHIER_LOG;
  unlink $fichier_lock;
  exit;
}

while ( my @data = $rtmp->fetchrow ) {
  my $container = $data[0];
  my $po_root = $data[1];
  my $fnd_initial_date = $data[2];
  my $sku_id = $data[3];
  my $stripped_sku = $data[4];
  my $stripped_ctn = $data[5]; 
  $rqlr->execute( $container, $po_root, $fnd_initial_date, $sku_id);
  if ( $dbh->errstr ne undef ) {
    print FICHIER_LOG $dbh->errstr."\n".$sqlr;
    $rqlr->finish; $rtmp->finish;
    $dbh->disconnect;
    close FICHIER_LOG;
    unlink $fichier_lock;
    exit;
  }  
  if ( $rqlr->rows == 1 ) { # SI UN SEUL ENREGISTREMENT
    my @result = $rqlr->fetchrow;
    if ( $result[1] eq '0' ) { # SI QUANTITES STRIPPEES NON RENSEIGNEES
      $rulr->execute( $stripped_sku, $stripped_ctn, $container, $result[0], $sku_id );
      if ( $dbh->errstr ne undef ) {
        print FICHIER_LOG $dbh->errstr."\n".$sulr;
        $rulr->finish;$rqlr->finish; $rtmp->finish;
        $dbh->disconnect;
        close FICHIER_LOG;
        unlink $fichier_lock;
        exit;
      }  
      print FICHIER_LOG "\n    ==>MISE A JOUR QTES STRIPPED POUR $container, $result[0], $fnd_initial_date, $sku_id : $stripped_sku pieces, $stripped_ctn cartons";
      $rulr->finish;
    } else {
      print FICHIER_LOG "\n    ==>PAS DE MISE A JOUR $container, $result[0], $fnd_initial_date, $sku_id : QUANTITES STRIPPEES DEJA RENSEIGNEES";
    }
  }
  elsif ( $rqlr->rows == 0 ) { # SI PAS D'ENREGISTREMENT
    print FICHIER_LOG "\n    ==>PAS DE MISE A JOUR $container, $po_root, $fnd_initial_date, $sku_id : AUCUN ENREGISTREMENT";
  } else {
    print FICHIER_LOG "\n    ==>PAS DE MISE A JOUR $container, $po_root, $fnd_initial_date, $sku_id: PLUSIEURS ENREGISTREMENTS";
  }
  $rqlr->finish;
}
$rtmp->finish;

# TRAITEMENT DES FICHIERS YR5_* 
# =============================
my $sh = " SELECT * FROM delivery_header WHERE num_do = ? AND nom_livraison =? "; 
my $rh = $dbh->prepare( $sh ); 

my $sih = " INSERT INTO delivery_header ( num_do, nom_livraison, date_depart, heure_depart, date_livraison_prevue, heure_livraison_prevue, num_commande, colis_reconditionnes, date_arrivee, heure_arrivee, date_start_loading, heure_start_loading, date_end_loading, heure_end_loading ) 
 VALUES ( ?, ?, ?, ?, ?, ?, ? ,? ,? ,? ,? ,? ,? ,? )
";
my $rih = $dbh->prepare( $sih ); 

my $sd1 = " SELECT TRIM(code_entrepot) FROM ref_entrepot WHERE code_wms = ? ";
my $rd1 = $dbh->prepare( $sd1 );
my $sd2 = " SELECT id_header, palettes, pieces, colis, poids FROM delivery_detail d
LEFT JOIN delivery_header h
ON h.id = d.id_header
WHERE TRIM(h.num_do) = ?
AND TRIM(h.nom_livraison) = ?
AND TRIM(d.num_bl) = ?
AND TRIM(d.num_po_root) = ?
AND TRIM(d.num_sku) = ?
AND TRIM(d.pays_bld) = ?
AND TRIM(d.reseau_bld) = ?
";
my $rd2 = $dbh->prepare( $sd2 );
my $sd3 = " INSERT INTO delivery_detail 
( id_header, num_bl, num_po_root, num_sku, pays_bld, reseau_bld, cpc_fcs, palettes, pieces, colis, poids )
VALUES( ( SELECT id FROM delivery_header WHERE num_do = ? AND nom_livraison = ? ), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
";
my $rd3 = $dbh->prepare( $sd3 );
my $sd4 = " UPDATE delivery_detail
SET palettes = ?,
pieces = ?,
colis = ?,
poids = ?
WHERE id_header = ?
AND num_bl = ?
AND num_po_root = ?
AND num_sku = ?
AND pays_bld = ?
AND reseau_bld = ?
";
my $rd4 = $dbh->prepare( $sd4 );
 
open(PIPE_LS, "/bin/ls YR5_* 2>/dev/null |");
while(<PIPE_LS>) {
  my $file_in = $_;
  $file_in =~ s/\n$//;
  $file_in =~ s/\r$//;

  # ON REGARDE SI LE TRANSFERT EST TERMINE ( ANALYSE DU FICHIER xferlog )
  my $transfert_OK = 0;
  open(FTP_LOG, "+< $file_log_ftp");
  while(<FTP_LOG>) {
    my $xtemp = $_;
    $xtemp =~ s/\n$//;
    $xtemp =~ s/\r$//;
    my @res_split = split(/\s+/, $xtemp); # $res_split[13] contient le login
    if ( $res_split[13] ne $login_bld ) { 
      next; 
    }
    if ( "/" . $file_in eq $res_split[8] and $res_split[17] eq "c" ) { # le Transfert est terminé
      $transfert_OK = 1;
      last;
    }
  }
  close(FTP_LOG);

  if ( $fcs_conf_yrocher::mode_debug ) { $transfert_OK = 1; }

  if ( $transfert_OK ) {
	print FICHIER_LOG "Début du traitement du fichier $file_in \n";
    my $num_do = '';
    my $nom_livraison = '';
    open(FILE_IN, $file_in);
    while(<FILE_IN>) {
      my $ligne = $_;
      if ( substr($ligne, 0, 1 ) eq 'E' ) {
        $num_do = &trimwhitespace(substr($ligne, 51, 15 ));
        $nom_livraison = &trimwhitespace(substr($ligne, 125, 35 ));
        my $num_commande = &trimwhitespace(substr($ligne, 41, 10 )); # le numéro e_entrepot
        #my $date_depart = &trimwhitespace(substr($ligne, 101, 8 ));
        #my $heure_depart = &trimwhitespace(substr($ligne, 953, 4 ));
        #my $date_livraison_prevue = &trimwhitespace(substr($ligne, 109, 8 ));
        #my $heure_livraison_prevue = &trimwhitespace(substr($ligne, 602, 4 ));
	my $date_arrivee = &trimwhitespace(substr($ligne, 957, 8 ));
	my $heure_arrivee = &trimwhitespace(substr($ligne, 965, 4 ));
	my $date_start_loading = &trimwhitespace(substr($ligne, 969, 8 ));
	my $heure_start_loading = &trimwhitespace(substr($ligne, 977, 4 ));
	my $date_end_loading = &trimwhitespace(substr($ligne, 981, 8 ));
	my $heure_end_loading = &trimwhitespace(substr($ligne, 989, 4 ));
	my $date_depart = &trimwhitespace(substr($ligne, 109, 8 ));
	my $heure_depart = &trimwhitespace(substr($ligne, 953, 4 ));
        my $date_livraison_prevue = &trimwhitespace(substr($ligne, 117, 8 ));
        my $heure_livraison_prevue = &trimwhitespace(substr($ligne, 602, 4 ));
	my $colis_reconditionnes = &trimwhitespace(substr($ligne, 949, 4 ));
        print FICHIER_LOG "\nTraitement $file_in : DO=$num_do LIVRAISON=$nom_livraison CAMION: Arrivée à $date_arrivee$heure_arrivee Start loading à $date_start_loading$heure_start_loading End loading = $date_end_loading$heure_end_loading Départ=$date_depart$heure_depart, Livraison prévue à $date_livraison_prevue$heure_livraison_prevue ";      
        $rh->execute( $num_do, $nom_livraison);
    	if ( $dbh->errstr ne undef ) {
	  print FICHIER_LOG $dbh->errstr."\n".$sh;
	  $rh->finish;
	  $dbh->disconnect;
	  close FICHIER_LOG;
          unlink $fichier_lock;
          exit;
	}
	if ( $rh->rows > 0 ) { # si l'enregistrement existe déjà : ON NE FAIT RIEN
	  $rh->finish;
          print FICHIER_LOG "\n	=> Le DO existe déjà, on ne crée pas l'header.";
	}
	else { # on insert
	  $rh->finish;
	  $rih->execute( $num_do, $nom_livraison, $date_depart, $heure_depart, $date_livraison_prevue, $heure_livraison_prevue, $num_commande, $colis_reconditionnes, $date_arrivee, $heure_arrivee, $date_start_loading, $heure_start_loading, $date_end_loading, $heure_end_loading );
    	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sih;
	    $rih->finish;
	    $dbh->disconnect;
	    close FICHIER_LOG;
            unlink $fichier_lock;
            exit;
	  }
		my $sqlr = "
		INSERT INTO delivery_header ( num_do, nom_livraison, date_depart, heure_depart, date_livraison_prevue, heure_livraison_prevue, num_commande, colis_reconditionnes, date_arrivee, heure_arrivee, date_start_loading, heure_start_loading, date_end_loading, heure_end_loading ) 
		VALUES ( '$num_do', '$nom_livraison', '$date_depart', '$heure_depart', '$date_livraison_prevue', '$heure_livraison_prevue', '$num_commande', '$colis_reconditionnes', '$date_arrivee', '$heure_arrivee', '$date_start_loading', '$heure_start_loading', '$date_end_loading', '$heure_end_loading' )
		";
          print FICHIER_LOG "\n	=> Insertion $sqlr";
 	  $rih->finish;
        }
      }
      else {
        print FICHIER_LOG "\n	=> Traitement du detail : ";
        if ( substr($ligne, 0, 1 ) eq 'D' ) {
          my $sku = &trimwhitespace(substr($ligne, 11, 15 )); # 
	  my $po_root = &trimwhitespace(substr($ligne, 26, 15 ));
          if ( index($po_root,'/') > 0 ) {
	    $po_root = substr($po_root, 0, index($po_root,'/') );
          }
	  my $pays = &trimwhitespace(substr($ligne, 51, 2 ));
	  my $code_support = &trimwhitespace(substr($ligne, 167, 3 ));
	  my $bl = &trimwhitespace(substr($ligne, 170, 15 ));
	  my $palettes = &trimwhitespace(substr($ligne, 55, 5 ));
	  my $pieces = &trimwhitespace(substr($ligne, 67, 10 ));
	  my $colis = &trimwhitespace(substr($ligne, 60, 7 ));
	  my $poids = &trimwhitespace(substr($ligne, 77, 10 ));
	  my $cpc_fcs = '';
          print FICHIER_LOG "\n	=> BL= $bl, PO_ROOT= $po_root, SKU =$sku, PAYS= $pays, RESEAU= $code_support ";
          # ON RECHERCHE le Code Entrepot FCS
	  $rd1->execute( $pays.$code_support);
    	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sd1;
	    $rd1->finish; $dbh->disconnect;
	    close FICHIER_LOG; unlink $fichier_lock;
            exit;
	  }
	  $cpc_fcs = $rd1->fetchrow;
	  $rd1->finish;

          # ON RECHERCHE si des infos existent sur la clé
          $rd2->execute( $num_do, $nom_livraison, $bl, $po_root, $sku, $pays, $code_support );
    	  if ( $dbh->errstr ne undef ) {
	    print FICHIER_LOG $dbh->errstr."\n".$sd2;
	    $rd2->finish; $dbh->disconnect;
	    close FICHIER_LOG; unlink $fichier_lock;
            exit;
	  }
          my $id_header = '';
	  my $b_palettes = 0;
	  my $b_pieces = 0;
	  my $b_colis = 0;
	  my $b_poids = 0;
	  while ( my @tab = $rd2->fetchrow ) {
            $id_header = $tab[0];
	    $b_palettes = $tab[1];
	    $b_pieces = $tab[2];
	    $b_colis = $tab[3];
	    $b_poids = $tab[4];
	  }
	  $rd2->finish;
          $palettes += $b_palettes ;
	  $pieces += $b_pieces;
	  $colis += $b_colis;
	  $poids += $b_poids;
	  if ( $id_header eq '' ) { # ON INSERT
	    $rd3->execute( $num_do, $nom_livraison, $bl, $po_root, $sku, $pays, $code_support, $cpc_fcs, $palettes, $pieces, $colis, $poids );
    	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$sd3;
	      $rd3->finish; $dbh->disconnect;
	      close FICHIER_LOG; unlink $fichier_lock;
              exit;
	    }
	    $rd3->finish;
            print FICHIER_LOG "\n		=> Insertion PAL= $palettes, PCS= $pieces, CTN= $colis, KGS= $poids";
	  }
	  else { # ON UPDATE
	    $rd4->execute( $palettes, $pieces, $colis, $poids, $id_header, $bl, $po_root, $sku, $pays, $code_support );
    	    if ( $dbh->errstr ne undef ) {
	      print FICHIER_LOG $dbh->errstr."\n".$sd4;
	      $rd4->finish; $dbh->disconnect;
	      close FICHIER_LOG; unlink $fichier_lock;
              exit;
	    }
	    $rd4->finish;
            print FICHIER_LOG "\n		=> Update PAL= $palettes, PCS= $pieces, CTN= $colis, KGS= $poids";
	  }
        }
      }
    } # de WHILE
    close(FILE_IN);
    move($file_in, $directory_save.$file_in);
  } # de transfert_OK
}
close(PIPE_LS);

# TRAITEMENT DES FICHIERS PDF 
# ===========================
my $sqlr = " SELECT TRIM(num_do) FROM delivery_header
		WHERE num_commande LIKE ?
";
my $rs = $dbh->prepare( $sqlr );

open(PIPE_LS, "/bin/ls *.pdf 2>/dev/null |");
while(<PIPE_LS>) {
  my $file_in = $_;
  $file_in =~ s/\n$//;
  $file_in =~ s/\r$//;

  # ON REGARDE SI LE TRANSFERT EST TERMINE ( ANALYSE DU FICHIER xferlog )
  my $transfert_OK = 0;
  open(FTP_LOG, "+< $file_log_ftp");
  while(<FTP_LOG>) {
    my $xtemp = $_;
    $xtemp =~ s/\n$//;
    $xtemp =~ s/\r$//;
    my @res_split = split(/\s+/, $xtemp); # $res_split[13] contient le login
    if ( $res_split[13] ne $login_bld ) { 
      next; 
    }
    if ( "/" . $file_in eq $res_split[8] and $res_split[17] eq "c" ) { # le Transfert est terminé
      $transfert_OK = 1;
      last;
    }
  }
  close(FTP_LOG);

  if ( $fcs_conf_yrocher::mode_debug ) { $transfert_OK = 1; }

  if ( $transfert_OK ) {
	print FICHIER_LOG "Début du traitement du fichier $file_in \n";
    my @res_split = split(/.pdf/, $file_in);
    my $num_commande = '';
    if ( substr($res_split[0],0,2) eq 'LC' ) { # Liste de colisage
      $num_commande = $res_split[0];
      my @res_tmp = split(/LC/, $num_commande);
      $num_commande = $res_tmp[1];
    }
    else { # Bon de livraison
      $num_commande = $res_split[0];
    }
    $rs->execute( '%'.$num_commande );
    my $num_do = $rs->fetchrow;
    $num_do =~ s/\s+//g;
    $rs->finish;

    # TEST SI NUM_DO CORRECT
    # Changé le 31 Février 2012
    #if ( $num_do ne '' && substr($num_do,0,1) eq '2' ) {
    if ( $num_do ne '' ) {
#print "\n$file_in	=> $file_out";
      my $file_out = '';
      if ( substr($res_split[0],0,2) eq 'LC' ) { # Liste de colisage
        $file_out = '/home/fcs/yrgroup/web/downloads/LC'.$num_do.'_'.$num_commande.'.pdf';
      }
      else { # Bon de livraison
        $file_out = '/home/fcs/yrgroup/web/downloads/'.$num_do.'_'.$num_commande.'.pdf';
      }
      print FICHIER_LOG "\nEnvoi $file_in vers $file_out\n";
      my $commande_mv        = '/bin/mv';
      `$commande_mv $file_in $file_out`;
      my $commande_chmod     = '/bin/chmod';
      `$commande_chmod 666 $file_out`;
    }
    else {
     # if ( $num_do eq '' ) {
        print FICHIER_LOG "\n$file_in : Pas de DO dans Road Tracing\n";
     # }else {
     #   print FICHIER_LOG "\n$file_in : Flux Russie, DO $num_do\n";
     # }
      move($file_in, $directory_save.$file_in);
    }
  }
}
close(PIPE_LS);

$tmp_date = now;
print FICHIER_LOG "\nFIN TRAITEMENT à $tmp_date\n";
close FICHIER_LOG;
$dbh->disconnect;

# ON SUPPRIMER LE FICHIER DE LOCK
unlink $fichier_lock;

sub send_mail_alerte() {
    my $serveur_mail = $fcs_serveur_mail_prod;
    my $To_destinataires =  'system@e-solutions.tm.fr';
    if ( $fcs_conf_yrocher::mode_debug ) {
      $serveur_mail = '192.168.0.50';
    }
    # On prepare le mail
    my $corps_message= "ATTENTION,\n l'intégration des flux BLD est bloquée.";
    my $mime_msg = MIME::Lite->new(
            From    => '<no-reply@fcsystem.com>',
            Subject => 'WARNING [ FCS ] Intégration des flux BLD',
            To      => $To_destinataires,
            Type    => 'TEXT',
            Data    => $corps_message
            )
    or print STDERR ("Erreur lors de la création de l'email : $!\n");

    if($mime_msg->send_by_smtp($serveur_mail)) {
    }
    else {
      print STDERR ("Erreur lors de l'envoi de l'email : $!\n");
    }
    return 1;
}
