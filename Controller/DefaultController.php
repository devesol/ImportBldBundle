<?php

namespace FCS\ImportBldBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction()
    {
        return $this->render('FCSImportBldBundle:Default:index.html.twig');
    }
}
