<?php
/**
 * @var $this CDbFixtureManager
 */
$fixtures = $this->getFixtures();

//get the table name
$fileNameExplode = explode( '.', basename( __FILE__, '.php' ) );
$tableName = $fileNameExplode[0];

//get the file
$rows = require( $fixtures[$tableName] );

//delete every row that matches one of our fixtures's rows
//to avoid duplicate key error or truncating the table
// @todo(add INSERT IGNORE to fixture manager?)

require_once( __DIR__ . '/../../models/Product.php' );

foreach( $rows as $row )
{
    /**
     * @var $model Product
     */
    $model = Product::model()
                    ->findByPk( $row[array_keys( $row )[0]] );

    if( $model != null )
    {
        $model->delete();
    }

}
return $rows;