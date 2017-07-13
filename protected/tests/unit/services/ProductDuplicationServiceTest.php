<?php

/**
 * Created by PhpStorm.
 * User: Lucas
 * Date: 17/05/2016
 * Time: 08:54
 */
class ProductDuplicationServiceTest extends CDbTestCase
{
    /**
     * @var $productDuplicationService ProductDuplicationService
     */
    private $productDuplicationService;

    /**
     * array of scores for situations.
     *
     * Because of the way data is passed around 'attribute_match' and 'attribute_mismatch' need to be by attributeTypeId
     *
     * @var array
     */
    private $_duplicateScores = array(
        'exact_name'            => 100,
        'edition_name_suffix'   => 80,
        'other_name_suffix'     => 0,
        'exact_date'            => 100,
        'same_month_games'      => 90,
        'same_year_games'       => 75,
        'different_date_games'  => 0,
        'same_year_movies'      => 80,
        'different_date_movies' => -50,
        'same_year_books'       => 66,
        'different_date_books'  => 50,
        'similar_ratings'       => 50,
        'dissimilar_ratings'    => -50,
        'attribute_match'       => array(),
        'attribute_mismatch'    => array(),
    );

    /**
     * array of scores for matching and mismatching attributes by attributeTypeName.
     *
     * Added to _duplicateScores by attributeTypeId during init()
     *
     * @var array
     */
    private $_attributeScoresArray = array(
        'attribute_match'    => array(
            'author'    => 150,
            'publisher' => 50,
            'cast'      => 100,
            'crew'      => 50,
            'developer' => 100,
            'genre'     => 25,
        ),
        'attribute_mismatch' => array(
            'author'    => -150,
            'publisher' => 0,
            'cast'      => -50,
            'crew'      => 0,
            'developer' => -100,
            'genre'     => 0
        ),
    );

    /**
     * array of root categories
     */
    private $rootCategories = array(
        'books'  => 24101,
        'movies' => 24275,
        'games'  => 57068
    );

    /**
     * @var $fixtureManager CDbFixtureManager
     */
    private $fixtureManager;

    public function setUp()
    {
        //init the product duplication service
        $this->productDuplicationService = new ProductDuplicationService();
        $this->productDuplicationService->setDebug( false );
        $this->productDuplicationService->init( 'GB' );

        $this->fixtureManager = $this->getFixtureManager();
    }

    public function testCreateNewMasterProduct()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );

        $masterProductsReport = $this->productDuplicationService->createMasterProductsReport( array() );
        foreach( $masterProductsReport as $report )
        {
            //  assert empty with invalid data
            $this->assertEmpty( $report );
        }
        $masterProductsReport = $this->productDuplicationService->createMasterProductsReport( array(1) );
        foreach( $masterProductsReport as $report )
        {
            //  assert empty with invalid data
            $this->assertEmpty( $report );
        }

        $productArrayWithMasters = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttribute( 'product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_5' )
                                 ->getAttribute( 'product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                 ->getAttribute( 'product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_5' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                 ->getAttribute( 'product_id' ),
        );
        $expectedResponse = array(
            'master_product_ids'            => array(
                $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                     ->getAttribute( 'product_id' ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_5' )
                                     ->getAttribute( 'product_id' ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                     ->getAttribute( 'product_id' ),
            ),
            'not_duplicate_products'        => array(
                $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                     ->getAttribute( 'product_id' ) => array(
                    'product_id'                            => $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                                                                    ->getAttribute( 'product_id' ),
                    "resolved_as_not_duplicate_product_ids" => array(
                        $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                             ->getAttribute( 'related_product_id' ),
                    )
                )
            ),
            'subordinate_products'          => array(
                $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                     ->getAttribute( 'related_product_id' ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                     ->getAttribute( 'related_product_id' ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                     ->getAttribute( 'related_product_id' ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_5' )
                                     ->getAttribute( 'related_product_id' ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                     ->getAttribute( 'related_product_id' ),
            ),
            'conflicted_products'           => array(
                $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                     ->getAttribute( 'product_id' ) => array(
                    'product_id'             => $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                     ->getAttribute( 'product_id' ),
                    'conflicting_master_ids' => $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                     ->getAttribute( 'related_product_id' ),
                ),
                $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                     ->getAttribute( 'product_id' ) => array(
                    'product_id'             => $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                     ->getAttribute( 'product_id' ),
                    'conflicting_master_ids' => $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                     ->getAttribute( 'related_product_id' ),
                ),
            ),
            'most_common_master_product_id' => $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                                    ->getAttribute( 'product_id' ),
        );
        $masterProductsReport = $this->productDuplicationService->createMasterProductsReport( $productArrayWithMasters );
        $this->assertNotEmpty( $masterProductsReport );
        foreach( $masterProductsReport as $key => $value )
        {
            if( is_array( $value ) )
            {
                $this->assertEquals( sort( $expectedResponse[$key] ), sort( $value ) );
            }
            else
            {
                $this->assertEquals( $expectedResponse[$key], $value );
            }
        }

        //test with a complex case

    }

    public function testPreparePotentialDuplicatesAttributes()
    {
        //generate send data
        $this->fixtureManager->load( array('Product' => 'Product') );

        $inputProduct = $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( $this->fixtureManager->getRecord( 'Product', 'sample_3' )
                                                                                                                            ->getAttribute( 'product_id' ) );
        $potentialDuplicatesStart = require( __DIR__ . '/../../storedArrays/potentialDuplicates.php' );

        //prepare ids in a way the function expects them
        $potentialDuplicatesIdsArray = array();
        foreach( $potentialDuplicatesStart as $key => $array )
        {
            $potentialDuplicatesIdsArray[$array[array_keys( $array )[0]]] = $array[array_keys( $array )[0]];
        }
        $this->productDuplicationService->preparePotentialDuplicatesAttributes( $inputProduct, $potentialDuplicatesStart, $inputProduct['product_id'], $potentialDuplicatesIdsArray, $this->rootCategories['books'] );

        $attributesToBeSet = array(1, 3, 38);

        //assert the attributes where set for the input product
        $this->assertArrayHasKey( 'product_attributes', $inputProduct );
        foreach( $inputProduct['product_attributes'] as $attributeId => $attributeValue )
        {
            $attributeId = explode( ':', $attributeId )[1];
            $this->assertTrue( in_array( $attributeId, $attributesToBeSet ) );
            $this->assertTrue( !empty( $attributeValue ) );
        }
        foreach( $potentialDuplicatesStart as $identifier => $arrayValues )
        {
            if( isset( $arrayValues['product_attributes'] ) )
            {
                foreach( $arrayValues['product_attributes'] as $attributeId => $attributeValue )
                {
                    $attributeId = explode( ':', $attributeId )[1];
                    $this->assertTrue( in_array( $attributeId, $attributesToBeSet ) );
                    $this->assertTrue( !empty( $attributeValue ) );
                }

            }
        }

//reset and do the same for games
        $inputProduct = $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( $this->fixtureManager->getRecord( 'Product', 'sample_3' )
                                                                                                                            ->getAttribute( 'product_id' ) );
        $potentialDuplicatesStart = require( __DIR__ . '/../../storedArrays/potentialDuplicates.php' );

        //prepare ids in a way the function expects them
        $potentialDuplicatesIdsArray = array();
        foreach( $potentialDuplicatesStart as $key => $array )
        {
            $potentialDuplicatesIdsArray[$array[array_keys( $array )[0]]] = $array[array_keys( $array )[0]];
        }

        $this->productDuplicationService->preparePotentialDuplicatesAttributes( $inputProduct, $potentialDuplicatesStart, $inputProduct['product_id'], $potentialDuplicatesIdsArray, $this->rootCategories['games'] );

        $attributesToBeSet = array(3, 32);

        //assert the attributes where set for the input product
        $this->assertArrayHasKey( 'product_attributes', $inputProduct );
        foreach( $inputProduct['product_attributes'] as $attributeId => $attributeValue )
        {
            $attributeId = explode( ':', $attributeId )[1];
            $this->assertTrue( in_array( $attributeId, $attributesToBeSet ) );
            $this->assertTrue( !empty( $attributeValue ) );
        }
        foreach( $potentialDuplicatesStart as $identifier => $arrayValues )
        {
            if( isset( $arrayValues['product_attributes'] ) )
            {
                foreach( $arrayValues['product_attributes'] as $attributeId => $attributeValue )
                {
                    $attributeId = explode( ':', $attributeId )[1];
                    $this->assertTrue( in_array( $attributeId, $attributesToBeSet ) );
                    $this->assertTrue( !empty( $attributeValue ) );
                }

            }
        }

    }

    public function testFindPotentialDuplicatesBySimilarReviews()
    {

        //get the fixture manager
        $this->fixtureManager->resetTable( 'user_review' );;
        $this->fixtureManager->loadFixture( 'user_review' );

        $duplicateProductIdsArray = $duplicateAttempts = array(

            6369602 => 6369602,
            6414938 => 6414938,
            6431984 => 6431984,
            9238433 => 9238433,
            9242381 => 9242381,
            9285382 => 9285382,
            9295867 => 9295867,
            9305817 => 9305817,
            9320458 => 9320458,
            546897  => 546897,
        );
        $potentialDuplicateProductsArray = array();
        foreach( $duplicateAttempts as $productId )
        {
            $potentialDuplicateProductsArray['product_id:' . $productId] = $productId;
        }
        $duplicates = array();
        $this->productDuplicationService->findPotentialDuplicatesBySimilarReviews( $potentialDuplicateProductsArray, $duplicateAttempts, $duplicates, 364299 );

        $this->assertEquals( count( $potentialDuplicateProductsArray ), 9 );
        $this->assertEquals( count( $duplicateAttempts ), 9 );
        $this->assertEquals( count( $duplicates ), 1 );
        $this->assertEquals( array(546897), $duplicates );
    }

    public function testFindPotentialDuplicatesBySimilarRatings()
    {

        //get the fixture manager
        $this->fixtureManager->resetTable( 'user_rating' );;
        $this->fixtureManager->loadFixture( 'user_rating' );

        $duplicateProductIdsArray = $duplicateAttempts = array(

            6369602 => 6369602,
            6414938 => 6414938,
            6431984 => 6431984,
            9238433 => 9238433,
            9242381 => 9242381,
            9285382 => 9285382,
            9295867 => 9295867,
            9305817 => 9305817,
            9320458 => 9320458,
            546897  => 546897,
        );
        $potentialDuplicateProductsArray = array();
        foreach( $duplicateAttempts as $productId )
        {
            $potentialDuplicateProductsArray['product_id:' . $productId] = $productId;
        }
        $duplicates = array();
        $this->productDuplicationService->findPotentialDuplicatesBySimilarRatings( $potentialDuplicateProductsArray, $duplicateAttempts, $duplicates, 364299 );

        $this->assertEquals( count( $potentialDuplicateProductsArray ), 9 );
        $this->assertEquals( count( $duplicateAttempts ), 9 );
        $this->assertEquals( count( $duplicates ), 1 );

        $this->assertEquals( array(546897), $duplicates );
    }

    public function testDecideDuplicateProducts()
    {
        $potentialDuplicates = array(
            'pass' => array(),
            'fail' => array()
        );
        for( $i = 0; $i < 10; $i++ )
        {
            $potentialDuplicates['fail'][$i]['max_score'] = $potentialDuplicates['pass'][$i]['max_score'] = 100;
            $potentialDuplicates['fail'][$i]['duplicate_score'] = $i * 7.4;
            $potentialDuplicates['pass'][$i]['duplicate_score'] = 75 + $i * 2.5;
            $potentialDuplicatesPass[$i] = $potentialDuplicatesFail[$i] = $i;
        }
        $failArray = $this->productDuplicationService->decideDuplicateProducts( $potentialDuplicates['fail'], $potentialDuplicatesFail );
        $passArray = $this->productDuplicationService->decideDuplicateProducts( $potentialDuplicates['pass'], $potentialDuplicatesPass );
        $this->assertEquals( 0, count( $failArray ) );
        $this->assertEquals( 10, count( $passArray ) );
    }

    public function testScorePotentialDuplicatesByAttributes()
    {
        $inputProduct = $productWithMatches = $productWithoutMatches = array();
        $productWithMatches[1]['duplicate_score'] = $productWithoutMatches[1]['duplicate_score'] = $productWithMatches[1]['max_score'] = $productWithoutMatches[1]['max_score'] = 0;
        $expectedValueWithoutMatches = $expectedValueAllMatch = $totalPoints = 0;
        //set all comparable attributes to the same value
        foreach( $this->_attributeScoresArray['attribute_match'] as $key => $value )
        {
            $expectedValueAllMatch += $value;
            $expectedValueWithoutMatches += $this->_attributeScoresArray['attribute_mismatch'][$key];
            $key = ProductAttributeType::getIdFromName( $key );
            $productWithMatches[1]['product_attributes']['product_attribute_type:' . $key] = $inputProduct['product_attributes']['product_attribute_type:' . $key] = 'testTrue';
            $productWithoutMatches[1]['product_attributes']['product_attribute_type:' . $key] = 'testFalse';

        }
        $this->productDuplicationService->scorePotentialDuplicatesByAttributes( $inputProduct, $productWithMatches );
        $this->productDuplicationService->scorePotentialDuplicatesByAttributes( $inputProduct, $productWithoutMatches );
        $this->assertEquals( $expectedValueAllMatch, $productWithMatches[1]['duplicate_score'] );
        $this->assertEquals( $expectedValueWithoutMatches, $productWithoutMatches[1]['duplicate_score'] );
        $this->assertEquals( $expectedValueAllMatch, $productWithMatches[1]['max_score'] );
        $this->assertEquals( $expectedValueAllMatch, $productWithoutMatches[1]['max_score'] );

    }

    public function testMatchAttributeValueArrayArray()
    {
        //test for author
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayArray( array('no', 'v.value', 'no'), array('value value'), 1 ) );
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayArray( array('no', 'v. value', 'no'), array('value value'), 1 ) );
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayArray( array('no', 'v value', 'no'), array('value value'), 1 ) );
        $this->assertFalse( $this->productDuplicationService->matchAttributeValueArrayArray( array('no', 'f value', 'no'), array('value value'), 1 ) );

        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayArray( array('no', 'no', 'value'), array('value value'), 2 ) );
        $this->assertFalse( $this->productDuplicationService->matchAttributeValueArrayArray( array('no', 'no', 'no'), array('value value'), 1 ) );

    }

    public function testMatchAttributeValueArrayString()
    {
        $testArray = array(
            1 => 'value',
            2 => 'notvalue'
        );
        //test for author
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayString( array('value'), 'value', 1 ) );
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayString( array('v. value'), 'value value', 1 ) );

        //test for anything else
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayString( array('string'), 'string', 2 ) );
        //assert true on partial 1
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayString( array('strin'), 'string', 3 ) );
        //assert true on partial 2
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayString( array('string'), 'strin', 4 ) );

        $testArray = array(
            1 => 'value',
            2 => 'notvalue'
        );
        //test with multiple values
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueArrayString( $testArray, 'value', 1 ) );

        $testArray = array(
            1 => 'no',
            2 => 'nonono'
        );
        $this->assertFalse( $this->productDuplicationService->matchAttributeValueArrayString( $testArray, 'value', 1 ) );

    }

    public function testMatchAttributeValueStringString()
    {
        //by author
        //assertTrue on exact match
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 'string', 'string', 1 ) );
        //assert true on partial 1
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 'strin', 'string', 1 ) );
        //assert true on partial 2
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 'string', 'strin', 1 ) );

        //test initial detection
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 's.s. test', 'string string test', 1 ) );
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 's s test', 'string string test', 1 ) );
        $this->assertFalse( $this->productDuplicationService->matchAttributeValueStringString( 's s test', 'test test test', 1 ) );
        $this->assertFalse( $this->productDuplicationService->matchAttributeValueStringString( 's.s. test', 'test test test', 1 ) );

        //by any other attributes
        //assertTrue on exact match
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 'string', 'string', 2 ) );
        //assert true on partial 1
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 'strin', 'string', 3 ) );
        //assert true on partial 2
        $this->assertTrue( $this->productDuplicationService->matchAttributeValueStringString( 'string', 'strin', 4 ) );
    }

    public function testScorePotentialDuplicateByCategory()
    {
        //generate send data
        $this->fixtureManager->load( array('Product' => 'Product') );

        $inputProduct = $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( $this->fixtureManager->getRecord( 'Product', 'sample_3' )
                                                                                                                            ->getAttribute( 'product_id' ) );
        $potentialDuplicatesStart = require( __DIR__ . '/../../storedArrays/potentialDuplicates.php' );
        $this->productDuplicationService->scorePotentialDuplicatesByCategory( $inputProduct, $potentialDuplicatesStart );

        $potentialDuplicatesWithCategoryMatch = array(
            223431,
            782472,
            811461,
            827977,
            858570,
            866622,
            909994,
            928901,
            1001097,
            1034811,
            1066934,
            1356863,
            1437112,
            1437830,
            1926114,
            5226065,
            6271737,
            6275357,
            6308645,
            13706985
        );

        // in this controlled sample the end result should be
        foreach( $potentialDuplicatesStart as $potentialDuplicateId => $potentialDuplicate )
        {
            if( in_array( $potentialDuplicate['product_id'], $potentialDuplicatesWithCategoryMatch ) )
            {
                $score = floor( 100 * $potentialDuplicate['num_shared_categories'] / min( $potentialDuplicate['num_categories'], $inputProduct['num_categories'] ) );
                $this->assertEquals( $score, $potentialDuplicate['duplicate_score'] );
            }
            else
            {
                $this->assertEquals( 0, $potentialDuplicate['duplicate_score'] );
            }
            $this->assertEquals( 100, $potentialDuplicate['max_score'] );
        }
    }

    public function testScorePotentialDuplicatesByReleaseDate()
    {
//generate send data
        $this->fixtureManager->load( array('Product' => 'Product') );

        $inputProduct = $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( $this->fixtureManager->getRecord( 'Product', 'sample_3' )
                                                                                                                            ->getAttribute( 'product_id' ) );
        $potentialDuplicatesStart = require( __DIR__ . '/../../storedArrays/potentialDuplicates.php' );

        $sameDate = array(208693);
        $sameMonth = array(
            673611,
            10334645,
            10471545,
            10536300,
            13271706
        );
        $sameYear = array(
            240144,
            244012,
            259166,
            380349,
            455401,
            465302,
            476983,
            488794,
            546897,
            546899,
            630284,
            656673,
            765124,
            782472,
            806184,
            811456,
            811461,
            850256,
            851181,
            853253,
            858568,
            866622,
            870582,
            876426,
            885747,
            901542,
            914349,
            916635,
            928902,
            955997,
            1001101,
            1008659,
            1008660,
            1008665,
            1008666,
            1008667,
            1008670,
            1008673,
            1081544,
            1085618,
            1573873,
            2226102,
            2769251,
            2794237,
            2794325,
            2841564,
            2842353,
            3622422,
            3628220,
            5219738,
            5219739,
            5219741,
            5222533,
            5222534,
            5222536,
            5231588,
            5231589,
            5233814,
            5233817,
            5246347,
            6201362,
            6337498,
            6369602,
            6414938,
            6431984,
            9238433,
            9242381,
            9285382,
            9295867,
            9305817,
            9320458,
            9321699,
            9474675,
            9503525,
            9637134,
            9871348,
            9881409,
            9985316,
            10238794,
            10271216,
            10334640,
            10422208,
            10486896,
            10551694,
            10557237,
            10669608,
            10695607,
            10746665,
            10780476,
            10791623,
            10846307,
            10897719,
            11005866,
            13299899,
            13434352,
            13472752,
            13529680,

        );
        $potentialDuplicates = $potentialDuplicatesStart;
        //test books
        $this->productDuplicationService->scorePotentialDuplicatesByReleaseDate( $inputProduct, $potentialDuplicates, $this->rootCategories['books'] );
        foreach( $potentialDuplicates as $identifier => $potentialDuplicate )
        {
            if( !empty( $potentialDuplicate['release_date'] ) )
            {

                if( in_array( $potentialDuplicate['product_id'], $sameDate ) )
                {
                    $this->assertEquals( $this->_duplicateScores['exact_date'], $potentialDuplicate['duplicate_score'] );
                }
                elseif( in_array( $potentialDuplicate['product_id'], $sameMonth ) || in_array( $potentialDuplicate['product_id'], $sameYear ) )
                {

                    $this->assertEquals( $this->_duplicateScores['same_year_books'], $potentialDuplicate['duplicate_score'] );
                }
                else
                {

                    $this->assertEquals( $this->_duplicateScores['different_date_books'], $potentialDuplicate['duplicate_score'] );
                }
                $this->assertEquals( 100, $potentialDuplicate['max_score'] );
            }
        }

        //reset for movies
        $potentialDuplicates = $potentialDuplicatesStart;
        //test movies
        $this->productDuplicationService->scorePotentialDuplicatesByReleaseDate( $inputProduct, $potentialDuplicates, $this->rootCategories['movies'] );
        foreach( $potentialDuplicates as $identifier => $potentialDuplicate )
        {
            if( !empty( $potentialDuplicate['release_date'] ) )
            {

                if( in_array( $potentialDuplicate['product_id'], $sameDate ) )
                {
                    $this->assertEquals( $this->_duplicateScores['exact_date'], $potentialDuplicate['duplicate_score'] );
                }
                elseif( in_array( $potentialDuplicate['product_id'], $sameMonth ) || in_array( $potentialDuplicate['product_id'], $sameYear ) )
                {
                    $this->assertEquals( $this->_duplicateScores['same_year_movies'], $potentialDuplicate['duplicate_score'] );
                }
                else
                {
                    $this->assertEquals( $this->_duplicateScores['different_date_movies'], $potentialDuplicate['duplicate_score'] );
                }
                $this->assertEquals( 100, $potentialDuplicate['max_score'] );
            }
        }
        //reset for movies
        $potentialDuplicates = $potentialDuplicatesStart;
        //test movies
        $this->productDuplicationService->scorePotentialDuplicatesByReleaseDate( $inputProduct, $potentialDuplicates, $this->rootCategories['games'] );
        foreach( $potentialDuplicates as $identifier => $potentialDuplicate )
        {
            if( !empty( $potentialDuplicate['release_date'] ) )
            {

                if( in_array( $potentialDuplicate['product_id'], $sameDate ) )
                {
                    $this->assertEquals( $this->_duplicateScores['exact_date'], $potentialDuplicate['duplicate_score'] );
                }
                elseif( in_array( $potentialDuplicate['product_id'], $sameMonth ) )
                {
                    $this->assertEquals( $this->_duplicateScores['same_month_games'], $potentialDuplicate['duplicate_score'] );
                }
                elseif( in_array( $potentialDuplicate['product_id'], $sameYear ) )
                {
                    $this->assertEquals( $this->_duplicateScores['same_year_games'], $potentialDuplicate['duplicate_score'] );
                }
                else
                {
                    $this->assertEquals( $this->_duplicateScores['different_date_games'], $potentialDuplicate['duplicate_score'] );
                }
                $this->assertEquals( 100, $potentialDuplicate['max_score'] );
            }
        }
    }

    public function testFilterPotentialDuplicatesByName()
    {
        $sentProductsForComparison = array();
        //generate send data
        $this->fixtureManager->load( array('Product' => 'Product') );
        $inputProduct = $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( $this->fixtureManager->getRecord( 'Product', 'sample_4' )
                                                                                                                            ->getAttribute( 'product_id' ) );

        for( $i = 1; $i <= 3; $i++ )
        {
            $usedProduct = $this->fixtureManager->getRecord( 'Product', 'sample_' . $i );
            $sentProductsForComparison[] = array(
                'product_id'      => $usedProduct->getAttribute( 'product_id' ),
                'product_name'    => $usedProduct->getAttribute( 'product_name' ),
                'name_comparison' => explode( ' ', strtolower( $usedProduct->getAttribute( 'product_name' ) ) ),

                'name_comparison_count'       => count( explode( ' ', strtolower( $usedProduct->getAttribute( 'product_name' ) ) ) ),
                'product_description'         => $usedProduct->getAttribute( 'product_description' ),
                'product_name_soundex_lookup' => $usedProduct->getAttribute( 'product_name_soundex_lookup' ),
                'is_subproduct'               => $usedProduct->getAttribute( 'is_subproduct' ),
                'num_categories'              => count( CategoryProduct::model()
                                                                       ->findAllByAttributes( array('product_id' => $usedProduct->getAttribute( 'product_id' )) ) ),
                'release_date'                => $usedProduct->getAttribute( 'release_date' )
            );
        }

        //add  products with names to test various conditions
        //longer name with number at the end should be excluded
        $sentProductsForComparison[] = array(
            'name_comparison'             => array('alice', 'in', 'wonderland', 2),
            'name_comparison_count'       => 4,
            'product_id'                  => $usedProduct->getAttribute( 'product_id' ),
            'product_name'                => 'Alice in Wonderland 2',
            'product_description'         => $usedProduct->getAttribute( 'product_description' ),
            'product_name_soundex_lookup' => $usedProduct->getAttribute( 'product_name_soundex_lookup' ),
            'is_subproduct'               => $usedProduct->getAttribute( 'is_subproduct' ),
            'num_categories'              => count( CategoryProduct::model()
                                                                   ->findAllByAttributes( array('product_id' => $usedProduct->getAttribute( 'product_id' )) ) ),
            'release_date'                => $usedProduct->getAttribute( 'release_date' )
        );

        //longer name with edition at the end shouldn't be excluded
        $sentProductsForComparison[] = array(
            'name_comparison'             => array('alice', 'in', 'wonderland', 'extended', 'edition'),
            'name_comparison_count'       => 4,
            'product_id'                  => $usedProduct->getAttribute( 'product_id' ),
            'product_name'                => 'Alice in Wonderland Extended Edition',
            'product_description'         => $usedProduct->getAttribute( 'product_description' ),
            'product_name_soundex_lookup' => $usedProduct->getAttribute( 'product_name_soundex_lookup' ),
            'is_subproduct'               => $usedProduct->getAttribute( 'is_subproduct' ),
            'num_categories'              => count( CategoryProduct::model()
                                                                   ->findAllByAttributes( array('product_id' => $usedProduct->getAttribute( 'product_id' )) ) ),
            'release_date'                => $usedProduct->getAttribute( 'release_date' )
        );

        //shorter name with number at the end should be excluded
        $sentProductsForComparison[] = array(
            'name_comparison'             => array('alice', 4),
            'name_comparison_count'       => 2,
            'product_id'                  => $usedProduct->getAttribute( 'product_id' ),
            'product_name'                => 'Alice 4',
            'product_description'         => $usedProduct->getAttribute( 'product_description' ),
            'product_name_soundex_lookup' => $usedProduct->getAttribute( 'product_name_soundex_lookup' ),
            'is_subproduct'               => $usedProduct->getAttribute( 'is_subproduct' ),
            'num_categories'              => count( CategoryProduct::model()
                                                                   ->findAllByAttributes( array('product_id' => $usedProduct->getAttribute( 'product_id' )) ) ),
            'release_date'                => $usedProduct->getAttribute( 'release_date' )
        );

        //shorter name with edition at the end shouldn't be excluded
        $sentProductsForComparison[] = array(
            'name_comparison'             => array('alice', 'edition'),
            'name_comparison_count'       => 2,
            'product_id'                  => $usedProduct->getAttribute( 'product_id' ),
            'product_name'                => 'Alice Edition',
            'product_description'         => $usedProduct->getAttribute( 'product_description' ),
            'product_name_soundex_lookup' => $usedProduct->getAttribute( 'product_name_soundex_lookup' ),
            'is_subproduct'               => $usedProduct->getAttribute( 'is_subproduct' ),
            'num_categories'              => count( CategoryProduct::model()
                                                                   ->findAllByAttributes( array('product_id' => $usedProduct->getAttribute( 'product_id' )) ) ),
            'release_date'                => $usedProduct->getAttribute( 'release_date' )
        );

        $this->productDuplicationService->filterPotentialDuplicatesByName( $inputProduct, $sentProductsForComparison, $sentProductsForComparison );

        //we should have 3 products left after filtering(Alice in Wonderland, Alice in Wonderland and Alice in Wonderland extended edition
        $this->assertEquals( 3, count( $sentProductsForComparison ) );

    }

    public function testGetPotentialDuplicateProducts()
    {
        //set the data
        $this->fixtureManager->load( array('Product' => 'Product') );
        $inputProduct = $this->fixtureManager->getRecord( 'Product', 'sample_4' );
        $potentialDuplicatesArray = array();
        $productAssembler = new ProductAssemblerService( $this->productDuplicationService );
        $rootCategoryId = YiiItcher::app()->db->createCommand( "SELECT c.root_category_id FROM category_product cp
JOIN category c ON cp.category_id=c.category_id WHERE cp.product_id=:productId  LIMIT 1;" )

                                              ->queryScalar( array(":productId" => $inputProduct->getAttribute( 'product_id' )) );

        $potentialDuplicates = $this->productDuplicationService->getPotentialDuplicateProducts( $inputProduct->getAttribute( 'product_id' ), $inputProduct->getAttribute( 'product_name_soundex_lookup' ), $potentialDuplicatesArray, $rootCategoryId );

        //match root category and soundex lookup foreach item
        foreach( $potentialDuplicates as $potentialDuplicate )
        {
            $this->assertTrue( ( $inputProduct->getAttribute( 'product_name_soundex_lookup' ) === $potentialDuplicate['product_name_soundex_lookup'] ) );
        }

    }

    public function testGetAmazonAlternateVersions()
    {
        $this->assertEmpty( $this->productDuplicationService->getAmazonAlternateVersions( 1 ) );

        //get assert the product has 4 found alternate amazon versions
        $this->assertEquals( count( $this->productDuplicationService->getAmazonAlternateVersions( 262576, array() ) ), 3 );

        //get assert the product has 4 found alternate amazon versions
        $this->assertEquals( count( $this->productDuplicationService->getAmazonAlternateVersions( 262576, array(1091443, 9352637) ) ), 1 );

    }

    public function testCreateProductNameComparison()
    {
        //assert & will be converted to and
        $product = array('product_id' => 1, 'product_name' => 'test & testing');
        $expectedResult = array('test', 'and', 'testing');
        $this->productDuplicationService->createProductNameComparison( $product );
        $this->assertEquals( $product['name_comparison'], $expectedResult );

        //assert roman and english numerals will be replaces
        $product = array('product_id' => 1, 'product_name' => 'II three IV five');
        $expectedResult = array(2, 3, 4, 5);
        $this->productDuplicationService->createProductNameComparison( $product );
        $this->assertEquals( $product['name_comparison'], $expectedResult );

        //assert invalid characters, whitespace and namestop words will be removed
        $product = array('product_id' => 1, 'product_name' => '^*% a the test');
        $expectedResult = array('test');
        $this->productDuplicationService->createProductNameComparison( $product );
        $this->assertEquals( $product['name_comparison'], $expectedResult );

    }

    public function testGetInputProductDetailsForDuplicateDetection()
    {
        $this->assertFalse( $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( 'a' ) );

        /**
         * @var $usedProduct Product
         */
        $this->fixtureManager->load( array('Product' => 'Product') );
        $usedProduct = $this->fixtureManager->getRecord( 'Product', 'sample_4' );
        $retrievedDetails = $this->productDuplicationService->getInputProductDetailsForDuplicateDetection( $usedProduct->getAttribute( 'product_id' ) );
        $this->assertNotEmpty( $retrievedDetails );
        $expectedDetails = array(
            'name_comparison'             => array('alice', 'in', 'wonderland'),
            'product_id'                  => $usedProduct->getAttribute( 'product_id' ),
            'product_name'                => $usedProduct->getAttribute( 'product_name' ),
            'product_description'         => $usedProduct->getAttribute( 'product_description' ),
            'product_name_soundex_lookup' => $usedProduct->getAttribute( 'product_name_soundex_lookup' ),
            'is_subproduct'               => $usedProduct->getAttribute( 'is_subproduct' ),
            'num_categories'              => count( CategoryProduct::model()
                                                                   ->findAllByAttributes( array('product_id' => $usedProduct->getAttribute( 'product_id' )) ) ),
            'release_date'                => $usedProduct->getAttribute( 'release_date' )
        );

        foreach( $expectedDetails as $key => $value )
        {
            $this->assertEquals( $value, $retrievedDetails[$key] );
        }
    }

    public function testUpdateProduct()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //truncate queue table
        $sql = 'truncate y_msg_queue_' . $this->productDuplicationService->getDataProviderId() . '_' . ProductDuplicationService::MEDIUM_PRIORITY_QUEUE_ID;
        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'function' => 'updateProductQueued',
            'params'   => array(
                'product_id' => 1,
            )
        );

        //assert true response on queueing
        $this->assertTrue( $this->productDuplicationService->updateProduct( $params['params']['product_id'], true ) );

        //get the last message
        $sql = 'select * from y_msg_queue_' . $this->productDuplicationService->getDataProviderId() . '_' . ProductDuplicationService::MEDIUM_PRIORITY_QUEUE_ID . '  order by message_id desc limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( json_decode( $queryResult[0]['message'], true ), $params );

        $validConflictedMasters = $this->fixtureManager->getRecord( 'product_relation', 'sample_1' );
        //assert return false for no product id
        $this->assertFalse( $this->productDuplicationService->updateProduct( 'a', false ) );

        //assert return false for invalid product id (non existent)
        $this->assertFalse( $this->productDuplicationService->updateProduct( 1, false ) );

        //assert return false for invalid product id (not right category)
        $this->assertFalse( $this->productDuplicationService->updateProduct( 5321907, false ) );

        //assert return false for invalid product id (not right category)
        $this->assertFalse( $this->productDuplicationService->updateProduct( $validConflictedMasters->getAttribute( 'product_id' ), false ) );

    }

    public function testFindMasterDuplicateProducts()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //assert empty with invalid data
        $this->assertEmpty( $this->productDuplicationService->findMasterDuplicateProducts( array() ) );
        $this->assertEmpty( $this->productDuplicationService->findMasterDuplicateProducts( array(1) ) );

        $arrayWithMasterProducts = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'related_product_id' ),
            1
        );

        //assert only the product which is master is returned
        $this->assertEquals( array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'product_id' )
        ), $this->productDuplicationService->findMasterDuplicateProducts( $arrayWithMasterProducts ) );

    }

    public function testFindSubordinateDuplicateProducts()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //assert empty with invalid data
        $this->assertEmpty( $this->productDuplicationService->findSubordinateDuplicateProducts( array() ) );
        $this->assertEmpty( $this->productDuplicationService->findSubordinateDuplicateProducts( array(1) ) );

        $arrayWithMasterProducts = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'related_product_id' ),
            1
        );
        $expectedResult = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'related_product_id' ) => array(
                'product_id'        => $this->fixtureManager->getRecord( 'product_relation', 'sample_14' )
                                                            ->getAttribute( 'related_product_id' ),
                'master_product_id' => array(
                    $this->fixtureManager->getRecord( 'product_relation', 'sample_14' )
                                         ->getAttribute( 'product_id' )
                ),
            )
        );
        $result = $this->productDuplicationService->findSubordinateDuplicateProducts( $arrayWithMasterProducts );
        //assert only the product which is master is returned
        $this->assertEquals( $expectedResult, $result );
    }

    public function testFindConflictedDuplicateProducts()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );

        //assert empty with invalid data
        $this->assertEmpty( $this->productDuplicationService->findConflictedDuplicateProducts( array() ) );
        $this->assertEmpty( $this->productDuplicationService->findConflictedDuplicateProducts( array(1) ) );

        $arrayWithMasterProducts = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                 ->getAttribute( 'product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'related_product_id' ),
            1
        );
        $result = ( $this->productDuplicationService->findConflictedDuplicateProducts( $arrayWithMasterProducts ) );

        $expectedArray = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                 ->getAttribute( 'related_product_id' ) => array(
                'product_id'             => $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                 ->getAttribute( 'related_product_id' ),
                'conflicting_master_ids' => array(
                    $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                         ->getAttribute( 'product_id' )
                )
            )
        );
        //assert only the product which is master is returned
        $this->assertEquals( $expectedArray, $result );

    }

    public function testCreateDuplicateRelationship()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        $existingDuplicateRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_5' );
        //assert false on existing relationship
        $this->assertFalse( $this->productDuplicationService->createDuplicateRelationship( $existingDuplicateRelation->getAttribute( 'product_id' ), $existingDuplicateRelation->getAttribute( 'related_product_id' ), $existingDuplicateRelation->getAttribute( 'relationship_type' ) ) );

        //assert false using the same product id
        $this->assertFalse( $this->productDuplicationService->createDuplicateRelationship( $existingDuplicateRelation->getAttribute( 'product_id' ), $existingDuplicateRelation->getAttribute( 'product_id' ), $existingDuplicateRelation->getAttribute( 'relationship_type' ) ) );

        $existingNotDuplicateRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_9' );

        //reset fixtures
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );

        //assert false without forced overwrite
        $this->assertFalse( $this->productDuplicationService->createDuplicateRelationship( $existingNotDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'related_product_id' ), ProductDuplicationService::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID ) );

        //assert true with forced overwrite
        $this->assertTrue( $this->productDuplicationService->createDuplicateRelationship( $existingNotDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'related_product_id' ), ProductDuplicationService::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID, true ) );

        //assert false without forced overwrite
        $this->assertFalse( $this->productDuplicationService->createDuplicateRelationship( $existingNotDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'related_product_id' ), ProductDuplicationService::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID ) );

        //assert true with forced overwrite
        $this->assertTrue( $this->productDuplicationService->createDuplicateRelationship( $existingNotDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'related_product_id' ), ProductDuplicationService::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID, true ) );

        //assert false without 2 master products
        $this->assertFalse( $this->productDuplicationService->createDuplicateRelationship( $existingNotDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'related_product_id' ), ProductDuplicationService::MASTER_MASTER_RELATION_ID ) );

        //assert true without forced overwrite
        $this->assertFalse( $this->productDuplicationService->createDuplicateRelationship( $existingNotDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'product_id' ), ProductDuplicationService::MASTER_MASTER_RELATION_ID ) );

        //reset fixtures
        $this->fixtureManager->resetTable( 'product_relation' );
        $this->fixtureManager->loadFixture( 'product_relation' );

        //assert true with forced overwrite
        $this->assertTrue( $this->productDuplicationService->createDuplicateRelationship( $existingDuplicateRelation->getAttribute( 'product_id' ), $existingNotDuplicateRelation->getAttribute( 'product_id' ), ProductDuplicationService::MASTER_MASTER_RELATION_ID, true ) );

    }

    public function testCreateProductRelation()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //truncate queue table
        $sql = 'truncate ' . ProductDuplicationService::INSERT_QUEUE_NAME;

        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'product_id'         => $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                         ->getAttribute( 'product_id' ),
            'relationship_type'  => ProductDuplicationService::CONFLICTED_MASTER_RELATION_ID,
            'related_product_id' => $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                         ->getAttribute( 'related_product_id' )
        );

        //assert true response on queueing
        $this->assertTrue( $this->productDuplicationService->createProductRelation( $params['product_id'], $params['relationship_type'], $params['related_product_id'], true ) );

        //get the last message
        $sql = 'select * from ' . ProductDuplicationService::INSERT_QUEUE_NAME . '  order by message_id desc limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( json_decode( $queryResult[0]['message'], true ), $params );

        //assert true when creating a valid relationship
        //delete the existing relationship
        ProductRelation::model()
                       ->findByAttributes( $params )
                       ->delete();

        //assert the relationship was created
        $this->assertTrue( $this->productDuplicationService->createProductRelation( $params['product_id'], $params['relationship_type'], $params['related_product_id'] ) );

        //assert the last record matches our insert
        $lastCreatedModel = ProductRelation::model()
                                           ->findByPk( YiiItcher::app()->db->getLastInsertID() );
        foreach( $params as $key => $value )
        {
            $this->assertEquals( $value, $lastCreatedModel->getAttribute( $key ) );
        }

        //assert a relation won't be created where one already exists
        $existingRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_5' );
        $this->assertFalse( $this->productDuplicationService->createProductRelation( $existingRelation->getAttribute( 'product_id' ), $existingRelation->getAttribute( 'relationship_type' ), $existingRelation->getAttribute( 'related_product_id' ) ) );

        //assert false on a new relation with invalid products (FK constrains)
        $this->assertFalse( $this->productDuplicationService->createProductRelation( $existingRelation->getAttribute( 'product_id' ), $existingRelation->getAttribute( 'relationship_type' ), 2 ) );

        //assert a relation won't be created where one already exists
        $existingRelation2 = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' );
        $this->assertTrue( $this->productDuplicationService->createProductRelation( $existingRelation->getAttribute( 'product_id' ), $existingRelation->getAttribute( 'relationship_type' ), $existingRelation2->getAttribute( 'related_product_id' ) ) );

    }

    public function testDeleteDuplicateRelationship()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        $validRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_5' );
        $productId = $validRelation->getAttribute( 'product_id' );
        $relatedProductId = $validRelation->getAttribute( 'related_product_id' );
        //assert false when trying to delete a relationship with invalid alpha
        $this->assertFalse( $this->productDuplicationService->deleteDuplicateRelationship( 1, $relatedProductId, ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY ) );

        //assert false when trying to delete a relationship with invalid beta
        $this->assertFalse( $this->productDuplicationService->deleteDuplicateRelationship( $productId, 1, ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY ) );

        //assert false when trying to delete a relationship with not related valid alpha and beta
        $this->assertFalse( $this->productDuplicationService->deleteDuplicateRelationship( $productId, $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                                                                            ->getAttribute( 'product_id' ), ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY ) );

        //get reviews and ratings from master and sub
        $userReviewsMasterProductPreDelete = UserReview::model()
                                                       ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );
        $userRatingsMasterProductPreDelete = UserRating::model()
                                                       ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );

        //get the user reviews for the related product
        $userReviewsSubordinateProductPreDelete = UserReview::model()
                                                            ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'related_product_id' )) );
        $userRatingsSubordinateProductPreDelete = UserRating::model()
                                                            ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'related_product_id' )) );

        //get the sub product model
        $subproductProductModel = Product::model()
                                         ->findByPK( $validRelation->getAttribute( 'related_product_id' ) );

        //check the product was already archived
        $this->assertTrue( ( $subproductProductModel->getAttribute( 'archived' ) == 1 ) );

        //get the current relations to assert we're actually deleting something

        $existingRelations = $this->productDuplicationService->getProductRelations( $productId, true );
        $this->assertNotEmpty( $existingRelations );

        //delete the conflicting master relationship
        $this->productDuplicationService->deleteDuplicateRelationship( $productId, $relatedProductId, ProductDuplicationService::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID );

        //get the current relations to assert we actually deleted something
        $currentRelationships = $this->productDuplicationService->getProductRelations( $productId, true );
        $this->assertEmpty( $currentRelationships );

        //get the sub product model
        $subproductProductModelPreDelete = Product::model()
                                                  ->findByPK( $validRelation->getAttribute( 'related_product_id' ) );

        //check the product was already archived
        $this->assertTrue( ( $subproductProductModel->getAttribute( 'archived' ) == 1 ) );

        //get masters reviews and ratings after delete  //get reviews and ratings from master and sub
        $userReviewsMasterProductAfterDelete = UserReview::model()
                                                         ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );
        $userRatingsMasterProductAfterDelete = UserRating::model()
                                                         ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );

        $this->assertTrue( count( $userReviewsMasterProductAfterDelete ) == count( $userReviewsMasterProductPreDelete ) - count( $userReviewsSubordinateProductPreDelete ) );
        $this->assertTrue( count( $userRatingsMasterProductAfterDelete ) == count( $userRatingsMasterProductPreDelete ) - count( $userRatingsSubordinateProductPreDelete ) );

        //check the relation doesn't exist anymore
        $this->assertTrue( Productrelation::model()
                                          ->findByAttributes( array(
                                              'product_id'         => $validRelation->getAttribute( 'product_id' ),
                                              'related_product_id' => $validRelation->getAttribute( 'related_product_id' )
                                          ) ) == null );
        //get the sub product model
        $subproductProductModelAfterDelete = Product::model()
                                                    ->findByPK( $validRelation->getAttribute( 'related_product_id' ) );

        //check the product was already archived
        $this->assertTrue( ( $subproductProductModelAfterDelete->getAttribute( 'archived' ) == 0 ) );

        $this->assertEmpty( $this->productDuplicationService->getProductRelations( $productId ) );
        $this->assertEmpty( $this->productDuplicationService->getProductRelations( $relatedProductId ) );

    }

    public function testRemoveSubordinateRatingsAndReviewsFromMaster()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation', 'user_review' => 'UserReview', 'user_rating' => 'UserRating') );
        //invalid relation
        $this->assertFalse( $this->productDuplicationService->removeSubordinateRatingsAndReviewsFromMaster( 1, 2 ) );

        $validRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_5' );
        //invalid relation
        $this->assertFalse( $this->productDuplicationService->removeSubordinateRatingsAndReviewsFromMaster( $validRelation->getAttribute( 'product_id' ), 2 ) );

        //invalid relation
        $this->assertFalse( $this->productDuplicationService->removeSubordinateRatingsAndReviewsFromMaster( 1, $validRelation->getAttribute( 'related_product_id' ) ) );

        //get the user reviews for the product
        $userReviewsMasterProductPreDelete = UserReview::model()
                                                       ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );
        $userRatingsMasterProductPreDelete = UserRating::model()
                                                       ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );

        //check that reviews and ratings were actually retrieved
        $this->assertTrue( ( count( $userReviewsMasterProductPreDelete ) > 0 ) );
        $this->assertTrue( ( count( $userRatingsMasterProductPreDelete ) > 0 ) );

        //get the user reviews for the related product
        $userReviewsSubordinateProductPreDelete = UserReview::model()
                                                            ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'related_product_id' )) );
        $userRatingsSubordinateProductPreDelete = UserRating::model()
                                                            ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'related_product_id' )) );

        //check that reviews and ratings were actually retrieved
        $this->assertTrue( ( count( $userRatingsSubordinateProductPreDelete ) > 0 ) );
        $this->assertTrue( ( count( $userRatingsSubordinateProductPreDelete ) > 0 ) );

        //delete them from the master
        $this->productDuplicationService->removeSubordinateRatingsAndReviewsFromMaster( $validRelation->getAttribute( 'product_id' ), $validRelation->getAttribute( 'related_product_id' ) );

        //get the user reviews for the related product
        $userReviewsMasterProductAfterDelete = UserReview::model()
                                                         ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );
        $userRatingsMasterProductAfterDelete = UserRating::model()
                                                         ->findAllByAttributes( array('product_id' => $validRelation->getAttribute( 'product_id' )) );

        //check that reviews and ratings were actually retrieved
        $this->assertTrue( ( count( $userReviewsMasterProductAfterDelete ) == count( $userReviewsMasterProductPreDelete ) - count( $userReviewsSubordinateProductPreDelete ) ) );
        $this->assertTrue( ( count( $userRatingsMasterProductAfterDelete ) == count( $userRatingsMasterProductPreDelete ) - count( $userRatingsSubordinateProductPreDelete ) ) );
    }

    /**
     * @cover ProductDuplicationService::getMasterProductKnownDuplicates()
     */
    public function testGetMasterProductKnownDuplicates()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //assert empty for a product with no relations
        $this->assertEmpty( $this->productDuplicationService->getMasterProductKnownDuplicates( 1 ) );

        //get a valid relation
        $validRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_5' );
        $retrievedData = $this->productDuplicationService->getMasterProductKnownDuplicates( $validRelation->getAttribute( 'product_id' ) );
        $expectedResponse = array($validRelation->getAttribute( 'related_product_id' ));

        $this->assertEquals( $expectedResponse, $retrievedData );

        //assert all know duplicates and not duplicates are retrieved
        $validRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' );

        //assert values for subordinate of a master with more than one sub without recurse
        $expectedResponse = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttribute( 'related_product_id' ),
            $validRelation->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'related_product_id' ),
        );
        $retrievedData = $this->productDuplicationService->getMasterProductKnownDuplicates( $validRelation->getAttribute( 'product_id' ) );
        $this->assertEquals( sort( $expectedResponse ), sort( $retrievedData ) );

        //assert values for subordinate of a master with more than one sub with recurse
        $expectedResponse = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttribute( 'related_product_id' ),
            $validRelation->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_14' )
                                 ->getAttribute( 'related_product_id' ),
        );
        $retrievedData = $this->productDuplicationService->getMasterProductKnownDuplicates( $validRelation->getAttribute( 'product_id' ) );
        $this->assertEquals( sort( $expectedResponse ), sort( $retrievedData ) );

    }

    /**
     * @covers ProductDuplicationService::getKnownDuplicates()
     */
    public function testGetKnownDuplicates()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //assert empty for a product with no relations
        $this->assertEmpty( $this->productDuplicationService->getKnownDuplicateIds( 1 ) );

        //get a valid relation
        $validRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_5' );

        //assert empty for a product with no subordinates
        $this->assertEmpty( $this->productDuplicationService->getKnownDuplicateIds( $validRelation->getAttribute( 'related_product_id' ) ) );

        $expectedResponse = array($validRelation->getAttribute( 'related_product_id' ));

        //assert empty for a product with a single subordinate
        $this->assertEquals( $expectedResponse, $this->productDuplicationService->getKnownDuplicateIds( $validRelation->getAttribute( 'product_id' ) ) );

        $validRelation = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' );

        //assert values for subordinate of a master with more than one sub
        $expectedResponse = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttribute( 'related_product_id' )
        );

        $this->assertEquals( sort( $expectedResponse ), sort( $this->productDuplicationService->getKnownDuplicateIds( $validRelation->getAttribute( 'related_product_id' ) ) ) );

        //assert that the master product gets all three subs
        $expectedResponse[] = $validRelation->getAttribute( 'related_product_id' );

        $this->assertEquals( sort( $expectedResponse ), sort( $this->productDuplicationService->getKnownDuplicateIds( $validRelation->getAttribute( 'product_id' ) ) ) );
    }

    /***
     * @covers ProductDuplicationService::resolveConflictByMasterIdsQueued
     * * @incomplete
     * @skip
     */
    public function testResolveConflictByMasterIdsQueued()
    {
        //truncate queue table
        $sql = 'truncate y_msg_queue_' . $this->productDuplicationService->getDataProviderId() . '_' . ProductDuplicationService::MEDIUM_PRIORITY_QUEUE_ID;
        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'function' => 'resolveConflictByMasterIdsQueued',
            'params'   => array(
                'master_alpha_product_id' => 1,
                'master_beta_product_id'  => 2,
            )
        );

        //assert true response on queueing
        $this->assertTrue( $this->productDuplicationService->resolveConflictByMasterIdsQueued( $params['params'], true ) );

        //get the last message
        $sql = 'select * from y_msg_queue_' . $this->productDuplicationService->getDataProviderId() . '_' . ProductDuplicationService::MEDIUM_PRIORITY_QUEUE_ID . '  order by message_id desc limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( json_decode( $queryResult[0]['message'], true ), $params );

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //assert return false with invalid alpha product Id
        $this->assertFalse( $this->productDuplicationService->resolveConflictByMasterIdsQueued( $params['params'] ) );
        $validConflictedMasters = $this->fixtureManager->getRecord( 'product_relation', 'sample_1' );

        $params['params']['master_alpha_product_id'] = $validConflictedMasters->getAttribute( 'product_id' );

        //assert return false with invalid beta product Id
        $this->assertFalse( $this->productDuplicationService->resolveConflictByMasterIdsQueued( $params['params'] ) );

        //set the beta with a valid id
        $params['params']['master_beta_product_id'] = $validConflictedMasters->getAttribute( 'related_product_id' );

        //assert return true for valid conflicted masters
        $this->assertTrue( $this->productDuplicationService->resolveConflictByMasterIdsQueued( $params['params'] ) );
    }

    /***
     * @covers ProductDuplicationService::resolveConflictByMasterIds
     */
    public function testResolveConflictByMasterIds()
    {

        //truncate queue table
        $sql = 'truncate y_msg_queue_' . $this->productDuplicationService->getDataProviderId() . '_' . ProductDuplicationService::MEDIUM_PRIORITY_QUEUE_ID;

        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'function' => 'resolveConflictByMasterIdsQueued',
            'params'   => array(
                'master_alpha_product_id' => 1,
                'master_beta_product_id'  => 2,
            )
        );

        //assert true response on queueing
        $this->assertTrue( $this->productDuplicationService->resolveConflictByMasterIds( $params['params']['master_alpha_product_id'], $params['params']['master_beta_product_id'], true ) );

        //get the last message
        $sql = 'select * from y_msg_queue_' . $this->productDuplicationService->getDataProviderId() . '_' . ProductDuplicationService::MEDIUM_PRIORITY_QUEUE_ID . '  order by message_id desc limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( json_decode( $queryResult[0]['message'], true ), $params );
    }

    /**
     * @covers ProductDuplicationService::checkDuplicateRelationship
     */
    public function testCheckDuplicateRelationship()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        $relationToBeChecked = $this->fixtureManager->getRecord( 'product_relation', 'sample_1' );
        $productId = $relationToBeChecked->getAttribute( 'product_id' );
        $relatedProductId = $relationToBeChecked->getAttribute( 'related_product_id' );

        //assert true with no relationship type
        $this->assertTrue( $this->productDuplicationService->checkDuplicateRelationship( $productId, $relatedProductId ) );

        //assert true with relationship type id
        $this->assertTrue( $this->productDuplicationService->checkDuplicateRelationship( $productId, $relatedProductId, $relationToBeChecked->getAttribute( 'relationship_type' ) ) );

        //assert true with relationship type being a string
        $this->assertTrue( $this->productDuplicationService->checkDuplicateRelationship( $productId, $relatedProductId, ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY ) );

        //assert false with invalid rel type
        $this->assertTrue( $this->productDuplicationService->checkDuplicateRelationship( $productId, $relatedProductId, 17 ) );

        //assert false with invalid product
        $this->assertFalse( $this->productDuplicationService->checkDuplicateRelationship( $productId, 12 ) );
    }

    /**
     * @covers  ProductDuplicationService::processExistingRelations();
     */
    public function testProcessExistingRelations()
    {
        //truncate queue table
        $sql = 'truncate ' . ProductDuplicationService::QUERY_QUEUE_NAME;
        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'start'      => 500,
            'limit'      => 50,
            'queue_only' => false
        );

        //assert true response on queueing
        $this->assertTrue( $this->productDuplicationService->processExistingRelations( $params['start'], $params['limit'] ) );

        //get the last message
        $sql = 'select * from ' . ProductDuplicationService::QUERY_QUEUE_NAME . '  order by message_id desc limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( json_decode( $queryResult[0]['message'], true ), $params );

        //get a subordinate row and process it(set limit to 1 and start to the correct point)109331	Subordinate	{"master_product_id":"13549629","resolved_as_not_duplicate_product_ids":["96308"]}
        $this->productDuplicationService->processExistingRelations( 49256, 1, false );

        //get the last created relation
        $relation = ProductRelation::model()
                                   ->findByPk( YiiItcher::app()->db->getLastInsertID() );

        //check the relation data is correct
        $this->assertEquals( $relation->getAttribute( 'product_id' ), 109331 );
        $this->assertEquals( $relation->getAttribute( 'related_product_id' ), 96308 );
        $this->assertEquals( $relation->getAttribute( 'relationship_type' ), ProductDuplicationService::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID );

        //get a master row and process it(set limit to 1 and start to the correct point)12027873	Master	{"subordinate_product_ids":["786559"]}
        $this->productDuplicationService->processExistingRelations( 51641, 1, false );

        //get the last created relation
        $relation = ProductRelation::model()
                                   ->findByPk( YiiItcher::app()->db->getLastInsertID() );

        //check the relation data is correct
        $this->assertEquals( $relation->getAttribute( 'product_id' ), 12027873 );
        $this->assertEquals( $relation->getAttribute( 'related_product_id' ), 786559 );
        $this->assertEquals( $relation->getAttribute( 'relationship_type' ), ProductDuplicationService::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID );

        //get a master row with multiple subordinates and process it(set limit to 1 and start to the correct point)12000043	Master	{"subordinate_product_ids":[202880,"6192675","202878"]}
        $this->productDuplicationService->processExistingRelations( 51622, 1, false );

        //get the last 3 relations tat were created
        $sql = 'select product_id, relationship_type, related_product_id from product_relation order by product_relation_id desc limit 0, 3';
        $relatedProducts = YiiItcher::app()->db->createCommand( $sql )
                                               ->queryAll();

        $relatedArray = array(202880, 6192675, 202878);
        //check the relation data is correct
        foreach( $relatedProducts as $relatedProduct )
        {
            $this->assertEquals( $relatedProduct['product_id'], 12000043 );
            $this->assertEquals( $relatedProduct['relationship_type'], ProductDuplicationService::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID );
            $inArray = in_array( $relatedProduct['related_product_id'], $relatedArray );
            $this->assertTrue( $inArray );
        }

    }

    /**
     * @covers ProductDuplicationService::getProductRelations();
     * @covers ProductDuplicationService::sortProductRelations();
     */
    public function testGetProductRelations()
    {

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //get relations for a conflicted master product
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                                                 ->getAttribute( 'product_id' ) );
        $expectedRelations = array();
        $expectedRelations[ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                                                                  ->getAttribute( 'related_product_id' );
        $expectedRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_11' )
                                                                                                                   ->getAttribute( 'related_product_id' );

        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

        //attempt the same in the other direction
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                                                 ->getAttribute( 'related_product_id' ) );
        $expectedRelations = array();
        $expectedRelations[ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_1' )
                                                                                                                  ->getAttribute( 'product_id' );
        $expectedRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_12' )
                                                                                                                   ->getAttribute( 'related_product_id' );

        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

        //get relations for a product which is only master of another
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                                                                 ->getAttribute( 'product_id' ) );
        $expectedRelations = array();
        for( $i = 2; $i <= 4; $i++ )
        {
            $expectedRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_' . $i )
                                                                                                                       ->getAttribute( 'related_product_id' );
        }

        $expectedRelations[ProductDuplicationService::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_13' )
                                                                                                                          ->getAttribute( 'related_product_id' );

        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

        //attempt the same in the other direction, they should only have one master and it should be the same for all
        $relatedProductId = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                 ->getAttribute( 'product_id' );
        for( $i = 2; $i <= 4; $i++ )
        {
            $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_' . $i )
                                                                                                     ->getAttribute( 'related_product_id' ) );
            foreach( $relations as $relationType => $relatedProducts )
            {
                $this->assertEquals( $relatedProductId, $relatedProducts[0] );
            }
        }

        //get relations for a conflicted product
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                                                 ->getAttribute( 'product_id' ) );
        $expectedRelations = array();
        $expectedRelations[ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                                                                  ->getAttribute( 'related_product_id' );
        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

        //attempt the same in the other direction
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                                                 ->getAttribute( 'related_product_id' ) );
        $expectedRelations = array();
        $expectedRelations[ProductDuplicationService::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_8' )
                                                                                                                  ->getAttribute( 'product_id' );
        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

        //get relations for a not duplicate product with duplicates
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_9' )
                                                                                                 ->getAttribute( 'product_id' ) );
        $expectedRelations = array();
        $expectedRelations[ProductDuplicationService::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_9' )
                                                                                                                          ->getAttribute( 'related_product_id' );
        $expectedRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_10' )
                                                                                                                   ->getAttribute( 'related_product_id' );

        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

        //attempt the same in the other direction only for the not duplicate
        $relations = $this->productDuplicationService->getProductRelations( $this->fixtureManager->getRecord( 'product_relation', 'sample_9' )
                                                                                                 ->getAttribute( 'related_product_id' ) );
        $expectedRelations = array();
        $expectedRelations[ProductDuplicationService::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_9' )
                                                                                                                          ->getAttribute( 'product_id' );
        $expectedRelations[ProductDuplicationService::SUBORDINATE_MASTER_PRODUCT_ID_KEY][] = $this->fixtureManager->getRecord( 'product_relation', 'sample_14' )
                                                                                                                  ->getAttribute( 'product_id' );

        foreach( $relations as $relationType => $relatedProducts )
        {
            $this->assertEquals( sort( $relatedProducts ), sort( $expectedRelations[$relationType] ) );
        }

    }

    /**
     * @covers ProductDuplicationService::checkIsMaster();
     */
    public function testCheckIsMaster()
    {
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //assert false with invalid input type
        $this->assertFalse( $this->productDuplicationService->checkIsMaster( 'invalid' ) );

        //assert false with a product which isn't a master
        $this->assertFalse( $this->productDuplicationService->checkIsMaster( $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                                                                  ->getAttribute( 'related_product_id' ) ) );

        //assert true with a product which is master
        $this->assertTrue( $this->productDuplicationService->checkIsMaster( $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                                                                 ->getAttribute( 'product_id' ) ) );
    }

    /**
     * @covers ProductDuplicationService::deleteCachedRelations();
     */
    public function testDeleteCachedRelations()
    {
        $cacheProductId = 10;
        $cacheKey = ProductDuplicationService::PRODUCT_RELATIONS_CACHE_NAME . '::' . $cacheProductId;
        $cacheValue = array('test' => 'testData');
        $this->productDuplicationService->cache->set( $cacheKey, $cacheValue );

        //check the string was saved
        $this->assertEquals( $cacheValue, $this->productDuplicationService->cache->get( $cacheKey ) );
        //attempt to delete it
        $this->productDuplicationService->deleteCachedRelations( $cacheProductId );

        //since data was deleted cache should be false
        $this->assertFalse( $this->productDuplicationService->cache->get( $cacheKey ) );
    }
}