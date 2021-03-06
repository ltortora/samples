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
foreach( $rows as $row )
{
    /**
     * @var $model DataProviderProduct
     */
    $model = DataProviderProduct::model()
                                ->findByAttributes( array(
                                    array_keys( $row )[1] => $row[array_keys( $row )[1]],
                                    array_keys( $row )[2] => $row[array_keys( $row )[2]]
                                ) );

    if( $model != null )
    {
        $model->delete();
    }

}
return $rows;