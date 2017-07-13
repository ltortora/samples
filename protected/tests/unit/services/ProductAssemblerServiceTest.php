<?php

/**
 * Created by testCommand::GenerateTestSkeleton()
 * Date: 2016-06-14
 * Time: 04:21:57
 */
class ProductAssemblerServiceTest extends CDbTestCase
{
    protected $attributeMappings = array(
        'NumberOfPages'       => 'pages',
        'PublicationDate'     => 'published_date',
        'ISBN'                => 'isbn',
        'EISBN'               => 'eisbn',
        'urls'                => 'urls',
        'news'                => 'news',
        'release_date'        => 'release_date',
        'publishers'          => 'publisher',
        'developers'          => 'developer',
        'publishedDate'       => 'published_date',
        'related user'        => 'related_user',
        'relatedUser'         => 'related_user',
        'publisher'           => 'publisher',
        'pages'               => 'pages',
        'isbn'                => 'isbn',
        'format'              => 'bookFormat',
        'pageCount'           => 'pages',
        'ISBN_10'             => 'isbn',
        'ISBN_13'             => 'eisbn',
        'searchInfo'          => 'searchdata',
        'author'              => 'author',
        'release_dates'       => 'release_date',
        'releaseDate'         => 'releaseDate',
        'tracks'              => 'tracks',
        'Genres'              => 'genre',
        'genres'              => 'genre',
        'wikipedia'           => 'wikipedia',
        'artist'              => 'artist',
        'album'               => 'album',
        'spotify_uri'         => 'spotify_uri',
        'rotten_tomatoes_uri' => 'rotten_tomatoes_uri',
        'music_videos'        => 'music_videos',
        'about'               => 'about',
        'other_albums'        => 'other_albums',
        'other_singles'       => 'other_singles',
    );

    /**
     * @var $productAssemblerService ProductAssemblerService
     */
    private $productAssemblerService;

    /**
     * @var $productDuplicationService ProductDuplicationService
     */
    private $productDuplicationService;

    /**
     * @var $productDuplicationDataProvider ProductDuplicationDataProvider
     */
    private $productDuplicationDataProvider;

    /**
     * @var $fixtureManager CDbFixtureManager
     */
    private $fixtureManager;

    public function setUp()
    {

        $this->fixtureManager = $this->getFixtureManager();
    }

    /**
     * @covers ProductAssemblerService::assembleProductByProductId()
    @incomplete
     **/
    public function testAssembleProductByProductId()
    {

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //truncate queue table
        $sql = 'truncate ' . ProductAssemblerService::PRODUCT_ASSEMBLER_QUEUE_NAME;
        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'params' => array(
                'product_id' => 1,
            )
        );

        //assert true response on queueing
        $this->assertTrue( $productAssemblerService->assembleProductByProductId( 1, true ) );

        //get the last message
        $sql = 'select * from ' . ProductAssemblerService::PRODUCT_ASSEMBLER_QUEUE_NAME . ' limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( $queryResult[0]['message'], $params['params']['product_id'] );

        //assert true on invalid product id
        $this->assertTrue( $productAssemblerService->assembleProductByProductId( 'test', true ) );
        $this->assertTrue( $productAssemblerService->assembleProductByProductId( array(), true ) );
        $this->assertTrue( $productAssemblerService->assembleProductByProductId( 1.89, true ) );

    }

    /**
     * @covers ProductAssemblerService::assembleProductByProductIdQueued()
    @incomplete
     **/
    public function testAssembleProductByProductIdQueued()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //truncate queue table
        $sql = 'truncate ' . ProductAssemblerService::PRODUCT_ASSEMBLER_QUEUE_NAME;
        YiiItcher::app()->db->createCommand( $sql )
                            ->query();

        //test queueing
        $params = array(
            'product_id' => 1,
        );

        //assert true response on queueing
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( $params, true ) );

        //get the last message
        $sql = 'select * from ' . ProductAssemblerService::PRODUCT_ASSEMBLER_QUEUE_NAME . ' limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( $queryResult[0]['message'], $params['product_id'] );

        //assert false with invalid product id
        $params = array('product_id' => 'asd');
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( $params ) );
        $params = array('product_id' => array());
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( $params ) );
        $params = array('product_id' => 1.7);
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( $params ) );

        //assert true when a product with invalid id is sent
        $params = array('product_id' => 1);
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( $params ) );

        //create a product with no root category or dps, assert it gets deleted
        $productWithoutDP = new Product();
        $productWithoutDP->setAttributes( array(
            'product_name'                => 'Product Without DP',
            'product_description'         => 'Product Description',
            'product_link'                => null,
            'product_average_rating'      => 0,
            'product_weighted_rating'     => 0,
            'product_num_ratings'         => 0,
            'product_num_reviews'         => 0,
            'num_related_items'           => 0,
            'product_attributes'          => null,
            'product_attribute_type'      => null,
            'archived'                    => 0,
            'release_date'                => 0,
            'product_name_md5'            => null,
            'provider_float_1'            => null,
            'provider_float_2'            => null,
            'provider_int_1'              => null,
            'provider_int_2'              => null,
            'product_root_category_id '   => null,
            'product_name_soundex_lookup' => null,
            'last_updated'                => date( 'Y-m-d' )
        ) );

        $productWithoutDPId = $productWithoutDP->getPrimaryKey();
        $params = array('product_id' => $productWithoutDPId);
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( $params ) );
        $this->assertNull( Product::model()
                                  ->findByPk( $productWithoutDPId ) );

        $this->fixtureManager->load( array('product_relation' => 'ProductRelation', 'product' => 'Product') );
        //get a master product and delete it's categories and dps to force them to be populated by it's subs
        $masterProductWithoutRootCategoryId = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                                   ->getAttribute( 'product_id' );
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $masterProductWithoutRootCategoryId) );
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $masterProductWithoutRootCategoryId) );

        //category should be added and false returned to reprocess
        $this->assertFalse( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $masterProductWithoutRootCategoryId) ) );
        $this->assertNotNull( CategoryProduct::model()
                                             ->findAllByAttributes( array('product_id' => $masterProductWithoutRootCategoryId) ) );

        //get a product which is not master and do the same, it should be deleted
        $subordinateProductWithoutRootCategoryId = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                                        ->getAttribute( 'related_product_id' );
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $subordinateProductWithoutRootCategoryId) );

        //return true as the product should be deleted and removed from the queue
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $subordinateProductWithoutRootCategoryId) ) );
        $this->assertNull( Product::model()
                                  ->findByPk( $productWithoutRootCategoryId ) );

        //reset fixtures
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation', 'product' => 'Product') );

        //get a product with no relations and kindle to test handleProductWithoutRootCategoryWithAmazonDataProvider
        $productWithoutSubsWithAmazonId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                               ->getPrimaryKey();
        //delete the relations
        ProductRelation::model()
                       ->deleteAllByAttributes( array('related_product_id' => $productWithoutSubsWithAmazonId) );
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithoutSubsWithAmazonId) );

        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithoutSubsWithAmazonId) );
        //add kindle store, should return true after
        $amazonCategoryProductModel = new DataProviderProduct();

        $amazonCategoryProductModel->setAttributes( array(
            'data_provider_product_id' => 'testId',
            'data_provider_id'         => 1,
            'product_id'               => $productWithoutSubsWithAmazonId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );
        $amazonCategoryProductModel->save();
        $kindleCategoryProductModel = new CategoryProduct();

        $kindleCategoryProductModel->setAttributes( array(
            'category_id' => Category::getIdFromName( 'Kindle Store' ),
            'product_id'  => $productWithoutSubsWithAmazonId,
            'relevance'   => 0,
        ) );
        $kindleCategoryProductModel->save();
        //add amazon dp,
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithoutSubsWithAmazonId) ) );

        $kindleCategoryProductModel->delete();
        $amazonCategoryProductModel->delete();

        //reset fixtures
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation', 'product' => 'Product') );

        //get a product with no relations and amazon dp to test handleProductWithoutRootCategoryWithAmazonDataProvider
        $productWithoutSubsWithAmazonId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                               ->getPrimaryKey();
        //delete the relations
        ProductRelation::model()
                       ->deleteAllByAttributes( array('related_product_id' => $productWithoutSubsWithAmazonId) );

        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithoutSubsWithAmazonId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithoutSubsWithAmazonId) );
        //add amazon dp, should return true after
        $amazonCategoryProductModel = new DataProviderProduct();

        $amazonCategoryProductModel->setAttributes( array(
            'data_provider_product_id' => 'testId',
            'data_provider_id'         => 1,
            'product_id'               => $productWithoutSubsWithAmazonId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );
        $amazonCategoryProductModel->save();
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithoutSubsWithAmazonId) ) );

        //reset fixtures
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation', 'product' => 'Product') );

        //create product with discogs dp no product or sub product and assert deletion
        $productWithDiscogsDpAndNoParentOrSubId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                                       ->getPrimaryKey();

        //delete all records form product subproduct
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentOrSubId) );
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('subproduct_id' => $productWithDiscogsDpAndNoParentOrSubId) );
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentOrSubId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentOrSubId) );
        $discogsDpModel = new DataProviderProduct();
        $discogsDpModel->setAttributes( array(
            'data_provider_product_id' => 'testId',
            'data_provider_id'         => 25,
            'product_id'               => $productWithDiscogsDpAndNoParentOrSubId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );

        $discogsDpModel->save();

        //assert return true
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithDiscogsDpAndNoParentOrSubId) ) );
        $this->assertNull( Product::model()
                                  ->findByPk( $productWithDiscogsDpAndNoParentOrSubId ) );

        //reset fixture and add parent product
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation', 'product' => 'Product') );

        //create product with discogs dp no product or sub product and assert deletion
        $productWithDiscogsDpAndNoSubId = $this->fixtureManager->getRecord( 'product', 'sample_5' )
                                                               ->getPrimaryKey();
        $supplementaryProductId = $this->fixtureManager->getRecord( 'product', 'sample_1' )
                                                       ->getPrimaryKey();

        //delete all records form product subproduct and add a parent product
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoSubId) );
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('subproduct_id' => $productWithDiscogsDpAndNoSubId) );
        $parentSubproductModel = new ProductSubproduct();
        $parentSubproductModel->setAttributes( array('product_id' => $supplementaryProductId, 'subproduct_id' => $productWithDiscogsDpAndNoSubId) );
        $parentSubproductModel->save();
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoSubId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoSubId) );
        //assert return false
        $this->assertFalse( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithDiscogsDpAndNoSubId) ) );

        //create product with discogs dp no product or sub product and assert deletion
        $productWithDiscogsDpAndNoParentId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                                  ->getPrimaryKey();

        //delete all records form product subproduct and add a parent product
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId) );
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('subproduct_id' => $productWithDiscogsDpAndNoParentId) );
        $parentSubproductModel = new ProductSubproduct();
        $parentSubproductModel->setAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId, 'subproduct_id' => $supplementaryProductId) );
        $parentSubproductModel->save();
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId) );

        //assert return true as it should be dequeued
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithDiscogsDpAndNoParentId) ) );

        //assert true on product with discogs but no parent or child
        //create product with discogs dp no product or sub product and assert deletion
        //reset fixture and add parent product
        $this->fixtureManager->load( array('product' => 'Product') );
        $productWithDiscogsDpAndNoParentId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                                  ->getPrimaryKey();

        //delete all records form product subproduct and add a parent product
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId) );
        ProductSubproduct::model()
                         ->deleteAllByAttributes( array('subproduct_id' => $productWithDiscogsDpAndNoParentId) );
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithDiscogsDpAndNoParentId) );
        $discogsDpModel = new DataProviderProduct();
        $discogsDpModel->setAttributes( array(
            'data_provider_product_id' => 'testId',
            'data_provider_id'         => 25,
            'product_id'               => $productWithDiscogsDpAndNoParentId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );
        $discogsDpModel->save();

        //assert return true
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithDiscogsDpAndNoParentId) ) );
        $this->assertNull( Product::model()
                                  ->findByPk( $productWithDiscogsDpAndNoParentId ) );

        //assert true when only tmdbdp (14)
        $this->fixtureManager->load( array('product' => 'Product') );

        //create product with discogs dp no product or sub product and assert deletion
        $productWithTIMDBId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                   ->getPrimaryKey();

        //delete all records form product subproduct and add a parent product
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithTIMDBId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithTIMDBId) );
        $dpModel = new DataProviderProduct();
        $dpModel->setAttributes( array(
            'data_provider_product_id' => 'testId',
            'data_provider_id'         => 14,
            'product_id'               => $productWithTIMDBId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );
        $dpModel->save();
        $this->assertFalse( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithDiscogsDpAndNoSubId) ) );

        //assert true when only theGamesDatabase (17)
        $this->fixtureManager->load( array('product' => 'Product') );

        //create product with discogs dp no product or sub product and assert deletion
        $productWithTGDBId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                  ->getPrimaryKey();

        //delete all records form product subproduct and add a parent product
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithTGDBId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithTGDBId) );
        $dpModel = new DataProviderProduct();
        $dpModel->setAttributes( array(
            'data_provider_product_id' => $productWithTGDBId,
            'data_provider_id'         => 17,
            'product_id'               => $productWithTGDBId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );
        $dpModel->save();
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithTGDBId) ) );

        //assert true and delete with only rotten tomatoes(5)
        $this->fixtureManager->load( array('product' => 'Product') );

        //create product with discogs dp no product or sub product and assert deletion
        $productWithRTId = $this->fixtureManager->getRecord( 'product', 'sample_4' )
                                                ->getPrimaryKey();

        //delete all records form product subproduct and add a parent product
        //delete all categories
        CategoryProduct::model()
                       ->deleteAllByAttributes( array('product_id' => $productWithRTId) );
        //delete all DataProviderProduct
        DataProviderProduct::model()
                           ->deleteAllByAttributes( array('product_id' => $productWithRTId) );
        $dpModel = new DataProviderProduct();
        $dpModel->setAttributes( array(
            'data_provider_product_id' => $productWithTGDBId,
            'data_provider_id'         => 5,
            'product_id'               => $productWithRTId,
            'last_updated'             => date( 'Y-m-d' ),
        ) );
        $dpModel->save();
        $this->assertTrue( $productAssemblerService->assembleProductByProductIdQueued( array('product_id' => $productWithRTId) ) );
        $this->assertNull( Product::model()
                                  ->findByPk( $productWithRTId ) );
    }

    /**
     * @covers ProductAssemblerService::getAssemblerProductInformation()
     **/
    public function testGetAssemblerProductInformation()
    {

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //test for music artists
        $productModel = new Product();
        $this->fixtureManager->load( array('product' => 'Product') );
        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );
        $musicRootCategory = $productModel->getRootCategoryId( $validProduct->getPrimaryKey() );
        $productId = $validProduct->getPrimaryKey();
        //get thr product basic data
        $productData = $validProduct->getAttributes();

        //assert empty for invalid root category
        $this->assertEmpty( $productAssemblerService->getAssemblerProductInformation( $productId, 1 ) );

        //get product attributes
        $productAttributesSQL = "SELECT pa.id, pa.product_id, pa.product_attribute_value, pat.product_attribute_type_name FROM product_attribute pa
            JOIN product_attribute_type pat ON pa.product_attribute_type_id=pat.product_attribute_type_id WHERE pa.product_id = :productId;";
        $productAttributesArray = YiiItcher::app()->db->createCommand( $productAttributesSQL )
                                                      ->queryAll( true, array(
                                                          ':productId' => $productId
                                                      ) );
        //get data_provider_product
        $dataProviderProductSQL = "SELECT product_id, data_provider_id, data_provider_product_id,  product_name, product_description, product_attributes, last_updated
            FROM data_provider_product WHERE product_id =  :productId and data_provider_id not in(28 ,29, 36)";
        $dataProviderProductsArray = YiiItcher::app()->db->createCommand( $dataProviderProductSQL )
                                                         ->queryAll( true, array(
                                                             ':productId' => $productId
                                                         ) );

        //test product 1 returns the same data that was sent
        $returnedInfo = $productAssemblerService->getAssemblerProductInformation( $productId, $musicRootCategory );

        //validate product data
        foreach( $returnedInfo['product'][0] as $key => $value )
        {
            $this->assertEquals( $productData[$key], $value );
        }
        foreach( $returnedInfo['product_attribute'] as $key => $value )
        {
            $this->assertEquals( $productAttributesArray[$key], $value );
        }
        $this->assertEquals( count( $dataProviderProductsArray ), count( $returnedInfo['data_provider_product'] ) );

        //master product
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct',
            'product_relation'      => 'ProductRelation'
        ) );

        //get master product and its duplicates with attributes
        $usedRelations = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttributes(),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttributes(),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttributes(),
        );
        $productId = $usedRelations[0]['product_id'];
        $booksCategoryId = $productModel->getRootCategoryId( $productId );
        $combinedProducts['product'] = $productAssemblerService->getProductData( $productId );
        $combinedProducts['data_provider_product'] = $productAssemblerService->getDataProviderProductsArray( $productId );
        $combinedProducts['product_attribute'] = $productAssemblerService->getProductAttributesArray( $productId );

        foreach( $usedRelations as $relation )
        {
            $combinedProducts['product'] = array_merge( $combinedProducts['product'], $productAssemblerService->getProductData( $relation['related_product_id'] ) );
            $combinedProducts['data_provider_product'] = array_merge( $combinedProducts['data_provider_product'], $productAssemblerService->getDataProviderProductsArray( $relation['related_product_id'] ) );
            $combinedProducts['product_attribute'] = array_merge( $combinedProducts['product_attribute'], $productAssemblerService->getProductAttributesArray( $relation['related_product_id'] ) );
        }

        //test product 1 returns the same data that was sent
        $returnedInfo = $productAssemblerService->getAssemblerProductInformation( $productId, $booksCategoryId );

        foreach( $returnedInfo as $key => $array )
        {
            $this->assertEquals( sort( $combinedProducts[$key] ), sort( $array ) );
        }

        //anything else
        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct'
        ) );

        //get a product
        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );

        $productId = $validProduct->getPrimaryKey();
        $resultPinkFloyd = $productAssemblerService->getAssemblerProductInformation( $productId, $booksCategoryId );

        //basic product data check
        $expectedPinkFloydData = $validProduct->getAttributes();

        $productAssemblerService->debugMsg( json_encode( $resultPinkFloyd ) );
        //assert all retrieved attributes match fixture data
        foreach( $resultPinkFloyd['product'][0] as $resultProductDataKey => $resultProductData )
        {
            $this->assertEquals( $expectedPinkFloydData[$resultProductDataKey], $resultProductData );
        }

        $expectedAttributes = array();
        //get all attribute created for pink floyd in the fixtures
        for( $i = 1; $i <= 8; $i++ )
        {
            $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute', 'sample_' . $i )
                                                         ->getAttributes();
        }
        $this->assertEquals( sort( $expectedAttributes ),

            sort( $resultPinkFloyd['product_attribute'] ) );
        //set the expected data
        $expectedDataProviderData = array();
        for( $i = 1; $i <= 119; $i++ )
        {
            $expectedDataProviderData[] = $this->fixtureManager->getRecord( 'data_provider_product', 'pink_floyd_' . $i );
        }

        $this->assertEquals( sort( $expectedDataProviderData ), sort( $resultPinkFloyd['data_provider_product'] ) );

    }

    /**
     * @covers ProductAssemblerService::saveAssembledProduct()
    @incomplete
     **/
    public function testSaveAssembledProduct()
    {

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get fixtures
        $this->fixtureManager->load( array(
            'product' => 'Product',
        ) );
        //get a product with a valid id
        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );
        //force it as displayed
        $validProduct->setAttribute( 'display', 1 );
        //remove description
        $validProduct->setAttribute( 'product_description', '' );
        //change the name to the something to assert the word the gets placed at the end of the sort name
        $validProduct->setAttribute( 'product_name', 'The test' );
        $validProduct->save();

        //generate send data
        $sendData = array(
            'product_name'        => $validProduct->getAttribute( 'product_name' ),
            'product_description' => $validProduct->getAttribute( 'product_description' ),
            'product_name'        => $validProduct->getAttribute( 'product_name' ),
            //change other attributes to assert they get saved
            'product_link'        => 'test Link',
            "release_date"        => 20010101,
            "rank"                => 99999999,
            'product_attributes'  => array()
        );
        //check with no desc gets archived and desc set to no desc yet
        $productAssemblerService->saveAssembledProduct( $validProduct->getPrimaryKey(), $sendData );
        $productAfterSave = Product::model()
                                   ->findByPk( $validProduct->getPrimaryKey() )
                                   ->getAttributes();
        //assert product_sort_name
        $this->assertEquals( 'test, The', $productAfterSave['product_sort_name'] );
        //assert description
        $this->assertEquals( 'No description yet', $productAfterSave['product_description'] );
        //assert display
        $this->assertEquals( 0, $productAfterSave['display'] );
        //assert other attributes
        $this->assertEquals( 'test Link', $productAfterSave['product_link'] );
        $this->assertEquals( 20010101, $productAfterSave['release_date'] );
        $this->assertEquals( 99999999, $productAfterSave['rank'] );

        //check artist with no release date isn't displayed
        //get a product with a valid id
        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );
        //force it as displayed
        $validProduct->setAttribute( 'display', 1 );

        $validProduct->save();

        $productModel = new Product();

        //generate send data
        $sendData = array(
            'product_name'             => 'A test',
            'product_root_category_id' => $productModel->getRootCategoryId( $validProduct->getPrimaryKey() ),
            'product_description'      => $validProduct->getAttribute( 'product_description' ),
            //change other attributes to assert they get saved
            'product_link'             => 'newLink',
            "release_date"             => '',
            "rank"                     => 99999999,
            'product_attributes'       => array('album_id' => json_encode( array() ), 'single_id' => json_encode( array() ))
        );
        $productAssemblerService->saveAssembledProduct( $validProduct->getPrimaryKey(), $sendData );
        $productAfterSave = Product::model()
                                   ->findByPk( $validProduct->getPrimaryKey() )
                                   ->getAttributes();

        //assert product_sort_name
        $this->assertEquals( 'test, A', $productAfterSave['product_sort_name'] );
        //assert display
        $this->assertEquals( 0, $productAfterSave['display'] );
        //assert other attributes
        $this->assertEquals( 'newLink', $productAfterSave['product_link'] );
        $this->assertEquals( 0, $productAfterSave['release_date'] );
        $this->assertEquals( 99999999, $productAfterSave['rank'] );

        // check if the product is archived or has no images set it not to display
        $sendData = array(
            'product_name'             => 'A test',
            'product_root_category_id' => $productModel->getRootCategoryId( $validProduct->getPrimaryKey() ),
            'product_description'      => $validProduct->getAttribute( 'product_description' ),
            //change other attributes to assert they get saved
            'product_link'             => 'newLink',
            'archived'                 => 1,
            "release_date"             => '',
            "rank"                     => 99999999,
            'product_attributes'       => array('album_id' => json_encode( array() ), 'single_id' => json_encode( array() ))
        );
        $productAssemblerService->saveAssembledProduct( $validProduct->getPrimaryKey(), $sendData );
        //assert display
        $this->assertEquals( 0, $productAfterSave['display'] );

        $productModel = Product::model()
                               ->findByPk( 5248256 );
        //set archived and display to 1
        $productModel->setAttributes( array('archived' => 1, 'display' => 1) );

        $productModel->save();
        $sendData = $productModel->getAttributes();
        $sendData['product_attributes'] = array();
        // If it's the product for 'various' artists set archived to 1 and display to 0
        $productAssemblerService->saveAssembledProduct( 5248256, $sendData );

        $productModel = Product::model()
                               ->findByPk( 5248256 );

        $this->assertEquals( 1, $productModel->getAttribute( 'archived' ) );
        $this->assertEquals( 0, $productModel->getAttribute( 'display' ) );
    }

    /**
     * @covers ProductAssemblerService::isMusicArtist()
    public function testIsMusicArtist()
    * {
 *
* //attempt results with duplication service
        * $productDuplicationService = new ProductDuplicationService();
        * $productDuplicationService->init( 'GB' );
        * $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        * $productAssemblerService->init( 'GB' );
 *
* //delete cache to check query
        * $productAssemblerService->cacheDelete( 'ProductAssemblerService::IsMusicArtist::1' );
        * $this->assertFalse( $productAssemblerService->isMusicArtist( 1 ) );
 *
* //generate send data
        * $this->fixtureManager->load( array('product' => 'Product') );
        * $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );
        * //delete cache to check query
        * $productAssemblerService->cacheDelete( 'ProductAssemblerService::IsMusicArtist::' . $validProduct->getPrimaryKey() );
        * $this->assertTrue( $productAssemblerService->isMusicArtist( $validProduct->getPrimaryKey() ) );
        * //recheck with cache
        * $this->assertTrue( $productAssemblerService->isMusicArtist( $validProduct->getPrimaryKey() ) );
 *
* //delete cache to check query
        * $invalidProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd_sub_1' );
        * $productAssemblerService->cacheDelete( 'ProductAssemblerService::IsMusicArtist::' . $invalidProduct->getPrimaryKey() );
        * $this->assertFalse( $productAssemblerService->isMusicArtist( $invalidProduct->getPrimaryKey() ) );
        * //recheck with cache
        * $this->assertFalse( $productAssemblerService->isMusicArtist( $invalidProduct->getPrimaryKey() ) );
        * $invalidProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd_sub_2' );
        * $productAssemblerService->cacheDelete( 'ProductAssemblerService::IsMusicArtist::' . $invalidProduct->getPrimaryKey() );
        * $this->assertFalse( $productAssemblerService->isMusicArtist( $invalidProduct->getPrimaryKey() ) );
        * //recheck with cache
        * $this->assertFalse( $productAssemblerService->isMusicArtist( $invalidProduct->getPrimaryKey() ) );
     * }
     **/
    /**
     * @covers ProductAssemblerService::getMusicArtistProductInformation()
     **/
    public function testGetMusicArtistProductInformation()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct'
        ) );

        //get a product which is the most popular with its name and with an echonest dp(TT)
        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );

        $productId = $validProduct->getPrimaryKey();
        //set product as archived to assert it is marked as not archived after
        $validProduct->setAttribute( 'archived', 1 );
        $validProduct->save();
        $resultPinkFloyd = $productAssemblerService->getMusicArtistProductInformation( $productId );
        $this->assertEquals( Product::model()
                                    ->findByPk( $productId )
                                    ->getAttribute( 'archived' ), 0 );

        //basic product data check
        $expectedPinkFloydData = $validProduct->getAttributes();

        //assert all retrieved attributes match fixture data
        foreach( $resultPinkFloyd['product'][0] as $resultProductDataKey => $resultProductData )
        {
            $this->assertEquals( $expectedPinkFloydData[$resultProductDataKey], $resultProductData );
        }

        //get all attribute created for pink floyd in the fixtures
        for( $i = 1; $i <= 8; $i++ )
        {
            $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute', 'sample_' . $i )
                                                         ->getAttributes();
        }
        $this->assertEquals( sort( $expectedAttributes ), sort( $resultPinkFloyd['product_attribute'] ) );

        //get all dp data for pink floyd

        //set the expected data
        $expectedDataProviderData = array();
        for( $i = 1; $i <= 119; $i++ )
        {
            $expectedDataProviderData[] = $this->fixtureManager->getRecord( 'data_provider_product', 'pink_floyd_' . $i );
        }

        $this->assertEquals( sort( $expectedDataProviderData ), sort( $resultPinkFloyd['data_provider_product'] ) );

        //check that when finding a more popular product without echonest dp all data from other products is transferred
        //get the original product info(FF)
        $productWithoutEchonestRecord = $this->fixtureManager->getRecord( 'product', 'pink_floyd_no_echonest' );
        $productWithoutEchonest['original_data'] = $productAssemblerService->getMusicArtistProductInformation( $productWithoutEchonestRecord->getAttribute( 'product_id' ) );
        $productModel = Product::model()
                               ->findByPk( $productWithoutEchonestRecord->getAttribute( 'product_id' ) );
        //assert the product is archived
        $this->assertEquals( $productModel->getAttribute( 'archived' ), 1 );

        //set it as more popular
        $productModel->setAttributes( array('product_average_rating' => 5, 'product_num_ratings' => 1000) );
        $productModel->save();

        //rerun the function (TF)
        $productWithoutEchonest['expanded_data'] = $productAssemblerService->getMusicArtistProductInformation( $productWithoutEchonestRecord->getAttribute( 'product_id' ) );
        $productModel = Product::model()
                               ->findByPk( $productWithoutEchonestRecord->getAttribute( 'product_id' ) );
        //assert the product is not archived
        $this->assertEquals( $productModel->getAttribute( 'archived' ), 0 );

        //set the expected data as a merge from the original pink floyd product and the one without echonest
        $expectedResultCombined = array_merge( $productWithoutEchonest['original_data']['product_attribute'], $resultPinkFloyd['product_attribute'] );
        $expectedResultCombined = array_merge( $productWithoutEchonest['original_data']['data_provider_product'], $resultPinkFloyd['data_provider_product'] );

        //assert all retrieved attributes match fixture data

        $expectedAttributes = array();
        $this->assertEquals( sort( $expectedResultCombined ), sort( $productWithoutEchonest['expanded_data']['product_attribute'] ) );

        $this->assertEquals( sort( $expectedResultCombined ), sort( $productWithoutEchonest['expanded_data']['data_provider_product'] ) );

        //if it isn't the most popular but has echonest check archived, removed all dps other than echonest, and other product not archived (FT)
        //use the original pink floyd which now isn't the most popular but has echonest
        $productId = $this->fixtureManager->getRecord( 'product', 'pink_floyd' )
                                          ->getPrimaryKey();
        //set product as not archived to assert it is marked as not archived after
        $pinkFloydModel = Product::model()
                                 ->findByPk( $productId );
        $pinkFloydModel->setAttributes( array('archived' => 0, 'product_average_rating' => 0, 'product_num_ratings' => 0) );
        $pinkFloydModel->save();
        $noEchonestModel = Product::model()
                                  ->findByPk( $productWithoutEchonestRecord->getAttribute( 'product_id' ) );
        $noEchonestModel->setAttributes( array('archived' => 1, 'product_average_rating' => 5, 'product_num_ratings' => 1000) );

        $noEchonestModel->save();

        $resultPinkFloydWithNoPopularity = $productAssemblerService->getMusicArtistProductInformation( $productId );

        $productWithoutEchonestModel = Product::model()
                                              ->findByPk( $productWithoutEchonestRecord->getAttribute( 'product_id' ) );
        //assert the product is not  archived
        $this->assertEquals( $productWithoutEchonestModel->getAttribute( 'archived' ), 0 );

        //assert the other product was archived
        $pinkFloydModel = Product::model()
                                 ->findByPk( $this->fixtureManager->getRecord( 'product', 'pink_floyd' )
                                                                  ->getPrimaryKey() );

        //assert the product is   archived
        $this->assertEquals( $pinkFloydModel->getAttribute( 'archived' ), 1 );

        //check the other product has been queued for assembly
        $sql = 'select * from ' . ProductAssemblerService::PRODUCT_ASSEMBLER_QUEUE_NAME . ' order by message_id desc limit 0, 1';

        $queryResult = YiiItcher::app()->db->createCommand( $sql )
                                           ->queryAll();

        //assert the last inserted message has the correct data
        $this->assertEquals( json_decode( $queryResult[0]['message'], true ), $productWithoutEchonestRecord->getAttribute( 'product_id' ) );

        //assert only echonest dp remains in the result
        foreach( $resultPinkFloydWithNoPopularity['data_provider_product'] as $value )
        {
            $this->assertEquals( $value['data_provider_id'], 25 );
        }

        //reset fixtures
        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct'
        ) );

        //check a product with no echonest and no popularity gets archived
        $productWithNoEchonestOrPopularity = $this->fixtureManager->getRecord( 'product', 'pink_floyd_no_echonest' );
        $productWithNoEchonestOrPopularity->setAttribute( 'archived', 0 );
        $productWithNoEchonestOrPopularity->save();
        $productAssemblerService->getMusicArtistProductInformation( $productWithNoEchonestOrPopularity->getPrimaryKey() );
        //assert the product is   archived
        $this->assertEquals( Product::model()
                                    ->findByPk( $productWithNoEchonestOrPopularity->getPrimaryKey() )
                                    ->getAttribute( 'archived' ), 1 );
    }

    /**
     * @covers ProductAssemblerService::getProductData()
     **/
    public function testGetProductData()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );
        //get fixture product data
        $this->fixtureManager->load( array('product' => 'Product') );
        $productData = $this->fixtureManager->getRecord( 'product', 'sample_1' )
                                            ->getAttributes();

        $result = $productAssemblerService->getProductData( $productData['product_id'] );

        //assert all retrieved attributes match fixture data
        foreach( $result as $resultProductDataKey => $resultProductData )
        {
            $this->assertEquals( $resultProductData, $productData[$resultProductDataKey] );
        }

        //assert false for invalid product Id
        $this->assertFalse( $productAssemblerService->getProductData( 1 ) );

    }


    /**
     * @covers ProductAssemblerService::getAdminProductAttributesArray()
     **/
    public function testGetAdminProductAttributesArray()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get the product and it's attributes
        $this->fixtureManager->load( array('product_attribute_admin_override' => 'ProductAttributeAdminOverride') );

        $expectedAttributes = array();
        $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute_admin_override', 'lotr_cast' )
                                                     ->getAttributes();
        $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute_admin_override', 'lotr_credits' )
                                                     ->getAttributes();

        $productId = $expectedAttributes[0]['product_id'];

        $productAttributes = $productAssemblerService->getAdminProductAttributesArray( $productId );

        $this->assertEquals( sort( $productAttributes ), sort( $expectedAttributes ) );

        //reset expected attributes
        $expectedAttributes = array();

        //generate requiredAttributes array
        $requiredAttributes = array();

        //get 4 attribute created for pink floyd in the fixtures
        $expectedAttributes = $this->fixtureManager->getRecord( 'product_attribute_admin_override', 'lotr_cast' )
                                                   ->getAttributes();
        $productAttributes = $productAssemblerService->getProductAttributesArray( $productId, array($expectedAttributes['product_attribute_type_id']) );

        $this->assertEquals( sort( $productAttributes ), sort( $expectedAttributes ) );
    }

    /**
     * @covers ProductAssemblerService::getProductAttributesArray()
     **/
    public function testGetProductAttributesArray()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get the product and it's attributes
        $this->fixtureManager->load( array(
            'product_attribute_admin_override' => 'ProductAttributeAdminOverride',
            'product'                          => 'Product',
            'product_attribute'                => 'ProductAttribute'
        ) );

        $adminAttributes = array();
        $adminAttributes['cast'] = $this->fixtureManager->getRecord( 'product_attribute_admin_override', 'lotr_cast' )
                                                        ->getAttributes();
        $adminAttributes['crew'] = $this->fixtureManager->getRecord( 'product_attribute_admin_override', 'lotr_credits' )
                                                        ->getAttributes();

        $importedAttributes = array();
        $importedAttributes['cast'] = $this->fixtureManager->getRecord( 'product_attribute', 'lotr_cast' )
                                                           ->getAttributes();
        $importedAttributes['crew'] = $this->fixtureManager->getRecord( 'product_attribute', 'lotr_credits' )
                                                           ->getAttributes();

        $expectedAttributes = array();

        //check if an imported attribute is set by adminUser
        foreach( $importedAttributes as $productAttributeType => $values )
        {
            $cleanAdminData = array();
            if( isset( $adminAttributes[$productAttributeType] ) )
            {
                $jsonData = json_decode( $values['product_attribute_value'], true );
                foreach( $jsonData as $values )
                {
                    if( !isset( $values['exclude'] ) || $values['exclude'] != true )
                    {
                        $cleanAdminData[] = $values;
                    }
                }
            }
            $expectedAttributes[$productAttributeType] = $cleanAdminData;
        }

        $productId = $expectedAttributes[0]['product_id'];

        //assert cast and crew marked as excluded are not returned
        $productAttributes = $productAssemblerService->getAdminProductAttributesArray( $productId );

        $this->assertEquals( sort( $productAttributes ), sort( $expectedAttributes ) );

    }
    /**
     * @covers ProductAssemblerService::getImportedProductAttributesArray()
     **/
    public function testImportedGetProductAttributesArray()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get the product and it's attributes
        $this->fixtureManager->load( array('product' => 'Product', 'product_attribute' => 'ProductAttribute') );

        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );

        $productId = $validProduct->getPrimaryKey();
        $expectedAttributes = array();

        //get all attribute created for pink floyd in the fixtures
        for( $i = 1; $i <= 8; $i++ )
        {
            $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute', 'sample_' . $i )
                                                         ->getAttributes();
        }

        $productAttributes = $productAssemblerService->getImportedProductAttributesArray( $productId );

        $this->assertEquals( sort( $productAttributes ), sort( $expectedAttributes ) );

        //reset expected attributes
        $expectedAttributes = array();

        //generate requiredAttributes array
        $requiredAttributes = array();

        //get 4 attribute created for pink floyd in the fixtures
        for( $i = 1; $i <= 4; $i++ )
        {
            $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute', 'sample_' . $i )
                                                         ->getAttributes();
            $requiredAttributes[] = ProductAttributeType::getIdFromName( $this->fixtureManager->getRecord( 'product_attribute', 'sample_' . $i )
                                                                                              ->getAttribute( 'product_attribute_type_name' ) );

        }
        $productAttributes = $productAssemblerService->getImportedProductAttributesArray( $productId );

        $this->assertEquals( sort( $productAttributes ), sort( $expectedAttributes ) );

    }

    /**
     * @covers ProductAssemblerService::getDataProviderProductsArray()
     **/
    public function testGetDataProviderProductsArray()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get the product and it's attributes
        $this->fixtureManager->load( array('product' => 'Product', 'data_provider_product' => 'DataProviderProduct') );

        //get a product to test
        $validProductId = $this->fixtureManager->getRecord( 'product', 'pink_floyd_sub_2' )
                                               ->getPrimaryKey();

        //set the expected data
        $expectedDataProviderData = array();
        for( $i = 1; $i <= 6; $i++ )
        {
            $expectedDataProviderData[] = $this->fixtureManager->getRecord( 'data_provider_product', 'pink_floyd_album_' . $i );
        }
        $dataProviderProductData = $productAssemblerService->getDataProviderProductsArray( $validProductId );

        $this->assertEquals( sort( $dataProviderProductData ), sort( $expectedDataProviderData ) );

        //reset expected dps
        $expectedDataProviderData = array();
        $sentDps = array();
        for( $i = 1; $i <= 2; $i++ )
        {
            $expectedDataProviderData[] = $this->fixtureManager->getRecord( 'data_provider_product', 'pink_floyd_album_' . $i );
            $sentDps = $this->fixtureManager->getRecord( 'data_provider_product', 'pink_floyd_album_' . $i )
                                            ->getAttribute( 'data_provider_id' );
        }

        $this->assertEquals( sort( $dataProviderProductData ), sort( $expectedDataProviderData ) );

    }

    /**
     * @covers ProductAssemblerService::musicArtistInformationValidation()
     **/
    public function testMusicArtistInformationValidation()
    {

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //check with music artist validation
        //check band members
        $testArrays = require( __DIR__ . '/../../storedArrays/testMusicArtistInformationValidationArrays.php' );

        //test product 1 returns the same data that was sent
        $returnedInfo = $productAssemblerService->musicArtistInformationValidation( $testArrays['product_1'], $testArrays['product_1']['product'][0]['product_id'] );
        $this->assertEquals( count( $returnedInfo['data_provider_product'] ), count( $testArrays['product_1']['data_provider_product'] ) - 4 );

        //test product 2 only returns the echonest dp
        $returnedInfo = $productAssemblerService->musicArtistInformationValidation( $testArrays['product_2'], $testArrays['product_2']['product'][0]['product_id'] );
        $this->assertEquals( 1, count( $returnedInfo['data_provider_product'] ) );
        $this->assertEquals( 25, $returnedInfo['data_provider_product'][0]['data_provider_id'] );

        //test product 3 returns the same data that was sent
        $returnedInfo = $productAssemblerService->musicArtistInformationValidation( $testArrays['product_3'], $testArrays['product_3']['product'][0]['product_id'] );
        $this->assertEquals( count( $returnedInfo['data_provider_product'] ), count( $testArrays['product_3']['data_provider_product'] ) );

    }

    /**
     * @covers ProductAssemblerService::getIndividualProductInformation()
     **/
    public function testGetIndividualProductInformation()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct'
        ) );

        //get a product
        $validProduct = $this->fixtureManager->getRecord( 'product', 'pink_floyd' );

        $productId = $validProduct->getPrimaryKey();
        $resultPinkFloyd = $productAssemblerService->getIndividualProductInformation( $productId );

        //basic product data check
        $expectedPinkFloydData = $validProduct->getAttributes();

        //assert all retrieved attributes match fixture data
        foreach( $resultPinkFloyd['product'][0] as $resultProductDataKey => $resultProductData )
        {
            $this->assertEquals( $expectedPinkFloydData[$resultProductDataKey], $resultProductData );
        }

        $expectedAttributes = array();
        //get all attribute created for pink floyd in the fixtures
        for( $i = 1; $i <= 8; $i++ )
        {
            $expectedAttributes[] = $this->fixtureManager->getRecord( 'product_attribute', 'sample_' . $i )
                                                         ->getAttributes();
        }
        $this->assertEquals( sort( $expectedAttributes ),

            sort( $resultPinkFloyd['product_attribute'] ) );
        //set the expected data
        $expectedDataProviderData = array();
        for( $i = 1; $i <= 119; $i++ )
        {
            $expectedDataProviderData[] = $this->fixtureManager->getRecord( 'data_provider_product', 'pink_floyd_' . $i );
        }

        $this->assertEquals( sort( $expectedDataProviderData ), sort( $resultPinkFloyd['data_provider_product'] ) );

    }

    /**
     * @covers ProductAssemblerService::individualProductInformationValidation()
     **/
    public function testIndividualProductInformationValidation()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //check band members
        $testArrays = require( __DIR__ . '/../../storedArrays/testIndividualProductInformationValidation.php' );

        //test product 1 returns the same data that was sent
        $returnedInfo = $productAssemblerService->individualProductInformationValidation( $testArrays['product_1'], $testArrays['product_1']['product'][0]['product_id'] );
        $this->assertEquals( 1, count( $returnedInfo['product'] ) );
        $this->assertEquals( 1, count( $returnedInfo['product_attribute'] ) );
        $this->assertEquals( 1, count( $returnedInfo['data_provider_product'] ) );

    }

    /**
     * @covers ProductAssemblerService::getMasterProductInformation()
     **/
    public function testGetMasterProductInformation()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );
        //get the necessary fixtures
        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct',
            'product_relation'      => 'ProductRelation'
        ) );

        //get master product and its duplicates with attributes
        $usedRelations = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttributes(),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttributes(),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttributes(),
        );

        $combinedProducts['product'] = $productAssemblerService->getProductData( $usedRelations[0]['product_id'] );
        $combinedProducts['data_provider_product'] = $productAssemblerService->getDataProviderProductsArray( $usedRelations[0]['product_id'] );
        $combinedProducts['product_attribute'] = $productAssemblerService->getProductAttributesArray( $usedRelations[0]['product_id'] );

        foreach( $usedRelations as $relation )
        {
            $combinedProducts['product'] = array_merge( $combinedProducts['product'], $productAssemblerService->getProductData( $relation['related_product_id'] ) );
            $combinedProducts['data_provider_product'] = array_merge( $combinedProducts['data_provider_product'], $productAssemblerService->getDataProviderProductsArray( $relation['related_product_id'] ) );
            $combinedProducts['product_attribute'] = array_merge( $combinedProducts['product_attribute'], $productAssemblerService->getProductAttributesArray( $relation['related_product_id'] ) );
        }

        //assert the master also retrieves the duplicates' attributes
        $retrievedInfo = $productAssemblerService->getMasterProductInformation( $usedRelations[0]['product_id'] );

        foreach( $retrievedInfo as $key => $array )
        {
            $this->assertEquals( sort( $combinedProducts[$key] ), sort( $array ) );
        }

        //retry with productDuplicationDP
        //attempt results with duplication service
        $productDuplicationDataProvider = new ProductDuplicationDataProvider();
        $productDuplicationDataProvider->init( 'GB' );

        $productAssemblerService = new ProductAssemblerService( $productDuplicationDataProvider );
        $productAssemblerService->init( 'GB' );

        //assert the master also retrieves the duplicates' attributes
        $retrievedInfo = $productAssemblerService->getMasterProductInformation( $usedRelations[0]['product_id'] );

        //info should be the same as the other
        foreach( $retrievedInfo as $key => $array )
        {
            $this->assertEquals( sort( $combinedProducts[$key] ), sort( $array ) );
        }
    }

    /**
     * @covers ProductAssemblerService::masterProductInformationValidation()
     **/
    public function testMasterProductInformationValidation()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );
        //get the necessary fixtures
        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct',
            'product_relation'      => 'ProductRelation'
        ) );

        //get master product and its duplicates with attributes
        $usedRelations = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttributes(),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttributes(),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttributes(),
        );
        $masterInfo = array();
        $masterInfo = $productAssemblerService->getIndividualProductInformation( $usedRelations[0]['product_id'] );
        $processedCompiledInfo = $productAssemblerService->processRawProductData( $masterInfo );

        foreach( $usedRelations as $relation )
        {
            $infoArray = $productAssemblerService->getIndividualProductInformation( $relation['related_product_id'] );
            $processedCompiledInfo = array_merge( $processedCompiledInfo, $infoArray );
        }
        $returnedCombinedProducts = $productAssemblerService->masterProductInformationValidation( $processedCompiledInfo, $usedRelations[0]['product_id'] );

        $this->assertEquals( sort( $processedCompiledInfo['product_attribute'] ), sort( $returnedCombinedProducts['product_attribute'] ) );
        $this->assertEquals( sort( $processedCompiledInfo['data_provider_product'] ), sort( $returnedCombinedProducts['data_provider_product'] ) );
        $this->assertEquals( sort( $processedCompiledInfo['product'] ), sort( $returnedCombinedProducts['product'] ) );

        //attempt results with duplication dp
        $productDuplicationService = new ProductDuplicationDataProvider();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );
        $returnedCombinedProducts = $productAssemblerService->masterProductInformationValidation( $processedCompiledInfo, $usedRelations[0]['product_id'] );
        $this->assertEquals( sort( $processedCompiledInfo['product_attribute'] ), sort( $returnedCombinedProducts['product_attribute'] ) );
        $this->assertEquals( sort( $processedCompiledInfo['data_provider_product'] ), sort( $returnedCombinedProducts['data_provider_product'] ) );
        $this->assertEquals( sort( $processedCompiledInfo['product'] ), sort( $returnedCombinedProducts['product'] ) );

    }

    /**
     * @covers ProductAssemblerService::processRawProductData()
     **/
    public function testProcessRawProductData()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        $unprocessedProductAttributes = array();
        //set Values for all productAttribute Mapping
        foreach( $this->attributeMappings as $original => $replacement )
        {
            $unprocessedProductAttributes[$original] = $original;
        }
        //set a valid value for info
        $unprocessedProductAttributes['info'] = '[{"title":"Runtime","value":[95]}]';
        $unprocessedProductAttributes['related_user'] = array(590351 => 590351);

        //set values for attributes not in mapping that should be rejected
        $unprocessedProductAttributes['new string attribute'] = 'new string value';
        $unprocessedProductAttributes['new_numeric_attribute'] = 42;
        $unprocessedProductAttributes['new_array_attribute'] = array('array_key' => 'array_value');

        //set values that shouldn't be rejected
        $unprocessedProductAttributes['cast'] = '[{"id":"870873","name":"Spencer Jones","character":"Fredric"},{"id":"870874","name":"Laura Evans","character":"Juniper"}]';
        $unprocessedProductAttributes['other albums'] = array('test' => 'testvalue');

        $productAttributesArray = array();
        $id = 1;
        foreach( $unprocessedProductAttributes as $key => $value )
        {
            $productAttributesArray[] = array(
                'id'                          => $id,
                'product_id'                  => 1,
                'product_attribute_type_name' => $key,
                'product_attribute_value'     => $value
            );
            $id++;
        }
        $rejectedProductAttributes = array();

        $productInformation = array(
            'data_provider_product' => array(
                array(
                    'data_provider_id'         => 1,
                    'data_provider_product_id' => 1,
                    'product_id'               => 1,
                    'product_id'               => 1,
                    'is_subproduct'            => 0,
                    'release_date'             => 2012,
                    'product_name'             => 'Test',
                    'product_description'      => 'product_description'
                )
            ),
            'product'               => array(
                array(
                    'product_id'          => 1,
                    'is_subproduct'       => 0,
                    'release_date'        => 2012,
                    'product_name'        => 'Test',
                    'product_description' => 'product_description'
                )
            ),
            'product_attribute'     => $productAttributesArray
        );

        //prebuilt response
        $expectedResponse = json_decode( '{"product_name":{"1::1::1":"Test","0::product_id:1::1":"Test"},"product_description":{"1::1::1":"product_description","0::product_id:1::1":"product_description"},"release_date":{"0::product_id:1::1":2012},"product_attributes":{"0::attribute_id:1::1":{"NumberOfPages":"NumberOfPages"},"0::attribute_id:2::1":{"PublicationDate":"PublicationDate"},"0::attribute_id:3::1":{"ISBN":"ISBN"},"0::attribute_id:4::1":{"EISBN":"EISBN"},"0::attribute_id:5::1":{"urls":"urls"},"0::attribute_id:6::1":{"news":"news"},"0::attribute_id:7::1":{"release_date":"release_date"},"0::attribute_id:8::1":{"publishers":"publishers"},"0::attribute_id:9::1":{"developers":"developers"},"0::attribute_id:10::1":{"publishedDate":"publishedDate"},"0::attribute_id:11::1":{"related user":"related user"},"0::attribute_id:12::1":{"relatedUser":"relatedUser"},"0::attribute_id:13::1":{"publisher":"publisher"},"0::attribute_id:14::1":{"pages":"pages"},"0::attribute_id:15::1":{"isbn":"isbn"},"0::attribute_id:16::1":{"format":"format"},"0::attribute_id:17::1":{"pageCount":"pageCount"},"0::attribute_id:18::1":{"ISBN_10":"ISBN_10"},"0::attribute_id:19::1":{"ISBN_13":"ISBN_13"},"0::attribute_id:20::1":{"searchInfo":"searchInfo"},"0::attribute_id:21::1":{"author":"author"},"0::attribute_id:22::1":{"release_dates":"release_dates"},"0::attribute_id:23::1":{"releaseDate":"releaseDate"},"0::attribute_id:24::1":{"tracks":"tracks"},"0::attribute_id:25::1":{"Genres":"Genres"},"0::attribute_id:26::1":{"genres":"genres"},"0::attribute_id:27::1":{"wikipedia":"wikipedia"},"0::attribute_id:28::1":{"artist":"artist"},"0::attribute_id:29::1":{"album":"album"},"0::attribute_id:30::1":{"spotify_uri":"spotify_uri"},"0::attribute_id:31::1":{"rotten_tomatoes_uri":"rotten_tomatoes_uri"},"0::attribute_id:32::1":{"music_videos":"music_videos"},"0::attribute_id:33::1":{"about":{"description":"about"}},"0::attribute_id:34::1":{"other_albums":"other_albums"},"0::attribute_id:35::1":{"other_singles":"other_singles"},"0::attribute_id:36::1":{"info":[{"title":"Runtime","value":[95]}]},"0::attribute_id:37::1":{"related_user":[590351]},"0::attribute_id:38::1":{"new string attribute":"new string value"},"0::attribute_id:39::1":{"new_numeric_attribute":42},"0::attribute_id:40::1":{"new_array_attribute":{"array_key":"array_value"}},"0::attribute_id:41::1":{"cast":"[{\"id\":\"870873\",\"name\":\"Spencer Jones\",\"character\":\"Fredric\"},{\"id\":\"870874\",\"name\":\"Laura Evans\",\"character\":\"Juniper\"}]"},"0::attribute_id:42::1":{"other albums":{"test":"testvalue"}}}}', true );
        $rejectedAttributes = array();
        $rawProduct = $productAssemblerService->processRawProductData( $productInformation, $rejectedAttributes );
        $this->assertEquals( $expectedResponse, $rawProduct );
        $this->assertEmpty( $rejectedAttributes );

    }

    /**
     * @covers ProductAssemblerService::collectDataProviderProductData()
     **/
    public function testCollectDataProviderProductData()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );
        //test data
        $expectedIdentifier = '1::1::1';
        $rawProductData = $rejectedAttributes = array();
        $dataProviderProduct = array(
            'data_provider_id'         => 1,
            'data_provider_product_id' => 1,
            'product_id'               => 1,
            'product_id'               => 1,
            'is_subproduct'            => 0,
            'release_date'             => 2012,
            'product_name'             => 'Test',
            'product_description'      => 'product_description'
        );

        $productAssemblerService->collectDataProviderProductData( $dataProviderProduct, $rawProductData, $rejectedAttributes );;

        foreach( $rawProductData as $key => $value )
        {
            $this->assertEquals( array($expectedIdentifier => $dataProviderProduct[$key]), $value );
        }
    }

    /**
     * @covers ProductAssemblerService::processDataProviderProductAttributesArray()
     **/
    public function testProcessDataProviderProductAttributesArray()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        $unprocessedProductAttributes = array();
        //set Values for all productAttribute Mapping
        foreach( $this->attributeMappings as $original => $replacement )
        {
            $unprocessedProductAttributes[$original] = $original;
        }
        //set a valid value for info
        $unprocessedProductAttributes['info'] = '[{"title":"Runtime","value":[95]}]';
        $unprocessedProductAttributes['related_user'] = array(590351 => 590351);

        //set values for attributes not in mapping that should be rejected
        $unprocessedProductAttributes['new string attribute'] = 'new string value';
        $unprocessedProductAttributes['new_numeric_attribute'] = 42;
        $unprocessedProductAttributes['new_array_attribute'] = array('array_key' => 'array_value');

        //set values that shouldn't be rejected
        $unprocessedProductAttributes['cast'] = '[{"id":"870873","name":"Spencer Jones","character":"Fredric"},{"id":"870874","name":"Laura Evans","character":"Juniper"}]';
        $unprocessedProductAttributes['other albums'] = array('test' => 'testvalue');
        $rejectedProductAttributes = array();

        //check info about and related user values are reformatted if necessary
        $processedProductAttributes = $productAssemblerService->processDataProviderProductAttributesArray( $unprocessedProductAttributes, $rejectedProductAttributes );

        //check attributes not in ProductAttributeType::model()->getIdByName( $attributeName ) are rejected
        $expectedRejectedAttributes = array(
            "bookFormat"            => "format",
            "searchdata"            => "searchInfo",
            "releaseDate"           => "releaseDate",
            "album"                 => "album",
            "new String Attribute"  => "new string value",
            "new_numeric_attribute" => 42,
            "new_array_attribute"   => array("array_key" => "array_value"),
            "other Albums"          => array("test" => "testvalue")
        );
        $this->assertEquals( $expectedRejectedAttributes, $rejectedProductAttributes );

        //check info about and related user values are reformatted if necessary
        $keysThatShouldBeReformatted = array(
            'about'        => array('description' => 'about'),
            'related_user' => array(590351),
            'info'         => array(array('title' => 'Runtime', 'value' => array(95)))
        );

        //check all processed attributes
        foreach( $processedProductAttributes as $key => $value )
        {
            //check  in attribute mappings are replaced
            if( in_array( $key, $this->attributeMappings ) && !in_array( $key, array_keys( $keysThatShouldBeReformatted ) ) )
            {
                $this->assertEquals( $key, $this->attributeMappings[$value] );
            }
            elseif( in_array( $key, array_keys( $keysThatShouldBeReformatted ) ) )
            {
                $this->assertEquals( $keysThatShouldBeReformatted[$key], $value );
            }
            //check the valid attributes where processed
            else
            {
                if( is_array( $value ) )
                {
                    $this->assertEquals( $value, json_decode( $unprocessedProductAttributes[$key], true ) );
                }
                else

                {
                    $this->assertEquals( $value, $unprocessedProductAttributes[$key] );
                }
            }
        }

    }

    /**
     * @covers ProductAssemblerService::collectDatabaseProductData()
     **/
    public function testCollectDatabaseProductData()
    {

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        $rawProductData = array();
        $attributesToBeSet = array(
            'product_id'          => 1,
            'is_subproduct'       => 0,
            'release_date'        => 2012,
            'product_name'        => 'Test',
            'product_description' => 'product_description'
        );
        $productAssemblerService->collectDatabaseProductData( $attributesToBeSet, $rawProductData );;
        $expectedKey = '0::product_id:1::1';
        foreach( $rawProductData as $key => $value )
        {
            $this->assertEquals( array($expectedKey => $attributesToBeSet[$key]), $value );
        }
    }

    /**
     * @covers ProductAssemblerService::formatRawProductData()
     **/
    public function testFormatRawProductData()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //assert basic attributes
        $rawProductData = array(
            "release_date"        => 2012,
            "product_name"        => "Test",
            "product_description" => "product_description",
            "is_subproduct"       => 1,
        );
        $formattedData = $productAssemblerService->formatRawProductData( $rawProductData );
        foreach( $formattedData as $key => $value )
        {
            $this->assertEquals( $value, $rawProductData[$key] );
        }

        //check that empty attributes are not set

        $rawProductData = array(
            "release_date",
            "product_name",
            "product_description",
            "is_subproduct"
        );
        $formattedData = $productAssemblerService->formatRawProductData( $rawProductData );

        $this->assertEquals( $formattedData, array("product_name" => array()) );

        //check product attributes
        //string and int should return themselves if not json
        //json strings should be returned decoded
        //multidimensional attributes should be returned in a multidimensiaonal array (subKey=>subValue)
        $rawProductData = array(
            'product_attributes' => array(
                'dpIdentifier1' => array(
                    'string'                => 'string1',
                    'int'                   => 1,
                    'validJson'             => json_encode( array('key1' => 'value1') ),
                    'multidimensionalArray' => array('info' => 'valueInfo1', 'title' => 'valueTitle1'),
                ),
                'dpIdentifier2' => array(
                    'string'                => 'string2',
                    'int'                   => 2,
                    'validJson'             => json_encode( array('key2' => 'value2') ),
                    'multidimensionalArray' => array('info' => 'valueInfo2', 'title' => 'valueTitle2')
                )
            )
        );

        $formattedData = $productAssemblerService->formatRawProductData( $rawProductData );
        $expectedFormattedData = array(
            'product_name'       => array(),
            'product_attributes' => array(
                'string'    => array('dpIdentifier1' => 'string1', 'dpIdentifier2' => 'string2',),
                'int'       => array('dpIdentifier1' => 1, 'dpIdentifier2' => 2,),
                'validJson' => array('key1' => array('dpIdentifier1' => 'value1'), 'key2' => array('dpIdentifier2' => 'value2'),),

                'multidimensionalArray' => array(
                    'info'  => array('dpIdentifier1' => 'valueInfo1', 'dpIdentifier2' => 'valueInfo2'),
                    'title' => array('dpIdentifier1' => 'valueTitle1', 'dpIdentifier2' => 'valueTitle2')
                )
            )
        );

        $this->assertEquals( $expectedFormattedData, $formattedData );

    }

    /**
     * @covers ProductAssemblerService::assembleProduct()
     **/
    public function testAssembleProduct()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //get fixtures
        $this->fixtureManager->load( array(
            'product'               => 'Product',
            'product_attribute'     => 'ProductAttribute',
            'data_provider_product' => 'DataProviderProduct'
        ) );

        $validProduct = $this->fixtureManager->getRecord( 'product', 'sample_3' );
        $productModel = new Product();
        $musicRootCategory = $productModel->getRootCategoryId( $validProduct->getPrimaryKey() );

        $rejectedAttributes = array();
        $validDataArray = $productAssemblerService->getAssemblerProductInformation( $validProduct->getPrimaryKey(), $musicRootCategory );

        $rawProduct = $productAssemblerService->processRawProductData( $validDataArray, $rejectedAttributes );

        $formattedRawProduct = $productAssemblerService->formatRawProductData( $rawProduct );
        $productAssemblerService->debugMsg( $formattedRawProduct );
        $assemblerProductProcessed = $productAssemblerService->assembleProduct( $formattedRawProduct, $assemblerProductId, $assemblerProductRootCategoryId/*, $rejectedAttributes*/ );
        $expectedResponse = array(
            "product_name"             => "Alice in Wonderland",
            "product_description"      => "An ordinary day becomes extraordinary when Alice follows the White Rabbit down a rabbit hole and finds herself in Wonderland. Join her as she meets one incredible character after another in this lively retelling of the classic quirky tale by Lewis Carroll.",
            "release_date"             => "20101126",
            "product_root_category_id" => null,
            "is_subproduct"            => 0,
            "product_attributes"       => array(
                "author"              => "Lesley Sims",
                "isbn"                => 1409527956,
                "pages"               => 24,
                "published_date"      => 2010,
                "publisher"           => "Usborne Publishing",
                "about"               => "{\"description\":\"An ordinary day becomes extraordinary when Alice follows the White Rabbit down a rabbit hole and finds herself in Wonderland. Join her as she meets one incredible character after another in this lively retelling of the classic quirky tale by Lewis Carroll.\"}",
                "product_description" => "An ordinary day becomes extraordinary when Alice follows the White Rabbit down a rabbit hole and finds herself in Wonderland. Join her as she meets one incredible character after another in this lively retelling of the classic quirky tale by Lewis Carroll.",
                "eisbn"               => 9781409527954,
                "kindle_product_id"   => 341747
            )
        );
        foreach( $expectedResponse as $responseKey => $response )
        {
            if( !is_array( $response ) )
            {
                $this->assertEquals( $response, $assemblerProductProcessed[$responseKey] );
            }
            else
            {
                foreach( $response as $key => $value )
                {
                    $this->assertEquals( $value, $assemblerProductProcessed[$responseKey][$key] );
                }
            }
        }
    }

    /**
     * @covers ProductAssemblerService::decideProductName()
     **/
    public function testDecideProductName()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //assert music artists use te discogs description if  set
        $nameArray = array('product_name' => array("25::1::1" => 'Discogs', "1::1::1" => 'Not discogs'));
        $selectedName = $productAssemblerService->decideProductName( $nameArray, 5321907, 155569 );
        $this->assertEquals( 'Discogs', $selectedName );

        //assert default name is used for music albums/singles/etc
        /*
         * @todo review
        $nameArray = array('product_name' => array("0::1::1" => 'Default Description', "1::1::1" => 'Not the default'));
        $selectedName = $productAssemblerService->decideProductName( $nameArray, 1, 155569 );
        $this->assertEquals( 'Not the default', $selectedName );*/

        //assert default is not  used for all other products
        $nameArray = array('product_name' => array("0::1::1" => 'Default Description', "1::1::1" => 'Not the default'));
        $selectedName = $productAssemblerService->decideProductName( $nameArray, 1, 57068 );
        $this->assertEquals( 'Not the default', $selectedName );
    }

    /**
     * @covers ProductAssemblerService::musicNameDecision()
     **/
    public function testMusicNameDecision()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );
        $nameArray = array("0::1::1" => 'name 0', "1::1::1" => 'name1');
        $selectedName = $productAssemblerService->musicNameDecision( $nameArray );
        //check with dp 0
        $this->assertEquals( 'name 0', $selectedName );
        //check regex work
        $nameArray = array("0::2::1" => 'name 0 by (Author) on Test', "0::1::1" => 'name1[][]');

        $selectedName = $productAssemblerService->musicNameDecision( $nameArray );
        $this->assertEquals( 'name 0', $selectedName );

        //check with discogs dp(25)
        //check regex work
        $nameArray = array("25::1::1" => 'name 0 by (Author) on Test', "1::1::1" => 'name1');
        $selectedName = $productAssemblerService->musicNameDecision( $nameArray );
        $this->assertEquals( 'name 0', $selectedName );

        //check with default
        $nameArray = array("24::1::1" => 'name 0 by (Author) on Test', "1::1::1" => 'name1');
        $selectedName = $productAssemblerService->musicNameDecision( $nameArray );
        $this->assertEquals( 'name 0', $selectedName );
    }

    /**
     * @covers ProductAssemblerService::defaultNameDecision()
     **/
    public function testDefaultNameDecision()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        $nameArray = array("product_name" => array("0::1::1" => 'name 0', "1::1::1" => 'name1'));

        //check that when ignore product names is set the default product name wont  be used
        $selectedName = $productAssemblerService->defaultNameDecision( $nameArray['product_name'], true );
        $this->assertEquals( 'name1', $selectedName );

        //check that when ignore product names is not set the default product name will be used if it's longer
        $selectedName = $productAssemblerService->defaultNameDecision( $nameArray['product_name'] );
        $this->assertEquals( 'name 0', $selectedName );

        //assert the most repeated name is used
        $nameArray = array("product_name" => array("0::1::1" => 'name 0', "1::1::1" => 'name1', "2::1::1" => 'name1'));
        $selectedName = $productAssemblerService->defaultNameDecision( $nameArray['product_name'] );
        $this->assertEquals( 'name1', $selectedName );

        //assert author and brackets are not counted for identification
        $nameArray = array("product_name" => array("0::1::1" => 'name 0[][]', "1::1::1" => 'name1 ()][.', "2::1::1" => 'name2 by (Author) on Test'));
        $selectedName = $productAssemblerService->defaultNameDecision( $nameArray['product_name'] );
        $this->assertEquals( 'name 0', $selectedName );

    }

    /**
     * @covers ProductAssemblerService::decideProductDescription()
     **/
    public function testDecideProductDescription()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //assert adminUser DP is returned if set
        $productData = array(
            'product_description' => array(
                '0::1::1'  => 'product table ',
                '36::1::2' => 'adminUser dp description  ',
                '12::1::2' => 'other longer description  '
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('0::1::3' => 'Wikipedia Introduction'))
            )
        );

        //product which is not music should return defaultDescriptionDecision();, if length < 32 => null
        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, 1, 1558 );

        $productData = array(
            'product_description' => array(
                '0::1::1' => 'product table ',
                '0::1::2' => 'other dp description  '
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('0::1::3' => 'Wikipedia Introduction'))
            )
        );

        //product which is not music should return defaultDescriptionDecision();, if length < 32 => null
        $descriptionDecision = $productAssemblerService->decideProductDescription( $productData, 1, 1558 );
        $this->assertNull( $descriptionDecision );

        $productData = array(
            'product_description' => array(
                '0::1::1' => 'product tableproduct tableproduct tableproduct tableproduct tableproduct table ',
                '0::1::2' => 'other dp description ther dp description ther dp description  '
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('0::1::3' => 'Wikipedia IntroductionWikipedia IntroductionWikipedia IntroductionWikipedia IntroductionWikipedia Introduction'))
            )
        );

        //product which is not music should return defaultDescriptionDecision();, if length > 32 => longest
        $descriptionDecision = $productAssemblerService->decideProductDescription( $productData, 1, 1558 );
        $this->assertEquals( $productData['product_attributes']['wikipedia']['Introduction']['0::1::3'], $descriptionDecision );

        $productData = array(
            'product_description' => array(
                '1::1::1' => 'product tableproduct tableproduct tableproduct tableproduct tableproduct table ',
                '2::1::2' => 'other dp description ther dp description ther dp description  '
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('3::1::3' => 'Wikipedia IntroductionWikipedia IntroductionWikipedia IntroductionWikipedia IntroductionWikipedia Introduction'))
            )
        );

        //assert musicMediaDescriptionDecision will return the longest description for music a category product which is an artist
        $descriptionDecision = $productAssemblerService->decideProductDescription( $productData, 5321907, 155569 );
        $this->assertEquals( $productData['product_attributes']['wikipedia']['Introduction']['3::1::3'], $descriptionDecision );

        $productData['product_attributes']['tracks']['1::1::1'] = json_decode( '{"1":{"duration":"1:26","position":"1","title":"Durango 95"},"2":{"duration":"1:30","position":"2","title":"Teenage Lobotomy"},"3":{"duration":"2:10","position":"3","title":"Psycho Therapy"},"4":{"duration":"1:36","position":"4","title":"Blitzkrieg Bop"},"5":{"duration":"3:00","position":"5","title":"Do You Remember Rock And Roll Radio"},"6":{"duration":"2:41","position":"6","title":"I Believe In Miracles"},"7":{"duration":"1:14","position":"7","title":"Gimme Gimme Shock Treatment"},"8":{"duration":"1:50","position":"8","title":"Rock \'N\' Roll High School"},"9":{"duration":"2:04","position":"9","title":"I Wanna Be Sedated"},"10":{"duration":"2:11","position":"10","title":"Spider-man"},"11":{"duration":"2:12","position":"11","title":"The K.K.K. Took My Baby Away"},"12":{"duration":"2:09","position":"12","title":"I Just Want To Have Something To Do"},"13":{"duration":"1:21","position":"13","title":"Commando"},"14":{"duration":"1:46","position":"14","title":"Sheena Is A Punk Rocker"},"15":{"duration":"2:11","position":"15","title":"Rockaway Beach"},"16":{"duration":"3:01","position":"16","title":"Pet Sematary"},"17":{"duration":"2:09","position":"17","title":"The Crusher"},"18":{"duration":"1:58","position":"18","title":"Love Kills"},"19":{"duration":"1:28","position":"19","title":"Do You Wanna Dance"},"20":{"duration":"2:31","position":"20","title":"Someone Put Something In My Drink"},"21":{"duration":"2:01","position":"21","title":"I Don\'t Want You"},"22":{"duration":"1:33","position":"22","title":"Wart Hog"},"23":{"duration":"1:22","position":"23","title":"Cretin Hop"},"24":{"duration":"1:17","position":"24","title":"R.A.M.O.N.E.S."},"25":{"duration":"1:40","position":"25","title":"Today Your Love, Tomorrow The World"},"26":{"duration":"2:57","position":"26","title":"Pinhead"},"27":{"duration":"1:56","position":"27","title":"53rd & 3rd"},"28":{"duration":"1:19","position":"28","title":"Listen To Your Heart"},"29":{"duration":"1:59","position":"29","title":"We\'re A Happy Family"},"30":{"duration":"2:32","position":"30","title":"Chinese Rock"},"31":{"duration":"2:14","position":"31","title":"Beat On The Brat"},"32":{"duration":"3:12","position":"32","title":"Any Way You Want It"}}', true );

        //assert musicMediaDescriptionDecision will be returned for music a category product which isn't artist, should add track listings if set
        $descriptionDecision = $productAssemblerService->decideProductDescription( $productData, 1, 155569 );
        $expectedDescription = $productData['product_attributes']['wikipedia']['Introduction']['3::1::3'];
        /*
                foreach( $productData ['product_attributes']['tracks']['1::1::1'] as $trackNumber => $trackData )
                {
                    $stringRegex = '/' . $trackData['position'] . '.*' . $trackData['title'] . '.*\(' . $trackData['duration'] . '\)/';
                    $matches = array();
                    preg_match( $stringRegex, $descriptionDecision, $matches );
                    $this->assertNotEmpty( $matches );
                }*/

        $this->assertTrue( strpos( $descriptionDecision, $expectedDescription ) === 0 );

        //assert discography will be added as description when description is null
        $productData = array(
            'product_description' => array(
                '0::1::1' => 'product tableproduct tableproduct tableproduct tableproduct tableproduct table ',
                '0::1::2' => 'other dp description ther dp description ther dp description  '
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('0::1::3' => 'Wikipedia IntroductionWikipedia IntroductionWikipedia IntroductionWikipedia IntroductionWikipedia Introduction')),
                'album_id'  => array(
                    '1::1::1' => array(
                        "9653833",
                        "9653834",
                        "9653836",
                        "9653838",
                        "9653839",
                        "9653841",
                        "9653842",
                        "9653843",
                        "9653845",
                        "9653846",
                        "9653848",
                        "9653849",
                        "9653851",
                        "9653855",
                        "9653856",
                        "9653857",
                        "9653858",
                        "9653859",
                        "9653867",
                        "9653869",
                        "9653872",
                        "9653873",
                        "9653877",
                        "11132397"
                    ),
                ),
                'single_id' => array(
                    '1::1::1' => array(
                        "9653835",
                        "9653837",
                        "9653840",
                        "9653844",
                        "9653847",
                        "9653850",
                        "9653852",
                        "9653853",
                        "9653854",
                        "9653860",
                        "9653861",
                        "9653862",
                        "9653863",
                        "9653864",
                        "9653865",
                        "9653866",
                        "9653868",
                        "9653870",
                        "9653871",
                        "9653874",
                        "9653875",
                        "9653876",
                        "9653878",
                        "9653879",
                        "9653880",
                        "9653881",
                        "9653882",
                        "9653883"
                    ),
                ),
            )
        );
        $descriptionDecision = $productAssemblerService->decideProductDescription( $productData, 5419452, 155569 );

        $this->assertTrue( strpos( $descriptionDecision, '[iHl]Most Recent Discography[/iHl]' ) === 0 );

    }

    /**
     * @covers ProductAssemblerService::defaultDescriptionDecision()
     **/
    public function testDefaultDescriptionDecision()
    {
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //assert adminUser DP is returned if set
        $productData = array(
            'product_description' => array(
                '0::1::1'  => 'product table description ',
                '36::1::2' => 'adminUser dp description  ',
                '12::1::2' => 'other longer description  '
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('0::1::3' => 'Wikipedia Introduction'))
            )
        );

        //product which is not music should return defaultDescriptionDecision();, if length < 32 => null
        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, ProductAssemblerService::musicRootId() , true );
        $this->assertEquals( $productData['product_attributes']['wikipedia']['Introduction']['36::1::3'], $descriptionDecision );

        $productData = array(
            'product_description' => array(
                '0::1::1' => 'product table description',
                '1::1::1' => 'other dp description'
            ),
            'product_attributes'  => array(
                'wikipedia' => array('introduction' => array('' => 'Wikipedia Introduction'))
            )
        );
        $productData['product_attributes']['wikipedia']['Introduction'][] = 'Wiki intro';

        //assert none of the descriptions ares used if $ignoreProductDescriptions is not null
        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, ProductAssemblerService::musicRootId() ,true );
        $this->assertNull( $descriptionDecision );

        //check no description is used if they're to short < 32
        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, ProductAssemblerService::musicRootId(),false );
        $this->assertNull( $descriptionDecision );

        $productData = array(
            'product_description' => array(
                '0::1::1' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus consequat at arcu et elementum. Nullam euismod libero ut suscipit volutpat. Maecenas porttitor vestibulum sem dignissim placerat. Sed ex sem, laoreet.',
                '1::1::1' => 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus consequat at arcu et elementum. Nullam euismod libero ut suscipit volutpat.'
            ),
            'product_attributes'  => array(
                'wikipedia' => array(
                    'Introduction' => array(
                        '2::1::1' => 'Wikipedia Introduction
Lorem ipsum dolor sit amet, '
                    )
                )
            )
        );
        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, ProductAssemblerService::musicRootId(),false );

        //assert the longest description is used regardless of the dp
        $this->assertEquals( $productData['product_description']['0::1::1'], $descriptionDecision );

        $productData = array(
            'product_description' => array(
                '0::1::1' => 'product table descriptionription',
                '1::1::1' => 'other dp  description description description  description description description description description  description description desc'
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('2::1::1' => 'Wikipedia Introduction  description description description  '))
            )
        );

        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, ProductAssemblerService::musicRootId(), false );

        //assert the longest description is used regardless of the dp
        $this->assertEquals( $productData['product_description']['1::1::1'], $descriptionDecision );
        $productData = array(
            'product_description' => array(
                '0::1::1' => 'product table descriptionription',
                '1::1::1' => 'other dp  description description description  description'
            ),
            'product_attributes'  => array(
                'wikipedia' => array('Introduction' => array('2::1::1' => 'Wikipedia Introduction  description description description  description description description description  description description desc'))
            )
        );

        $descriptionDecision = $productAssemblerService->defaultDescriptionDecision( $productData, ProductAssemblerService::musicRootId(),  false );

        //assert the longest description is used regardless of the dp
        $this->assertEquals( $productData['product_attributes']['wikipedia']['Introduction']['2::1::1'], $descriptionDecision );

    }

    /**
     * @covers ProductAssemblerService::decideReleaseDate()
     **/
    public function testDecideReleaseDate()
    {

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        $productData = array(
            'release_date' => array(
                '0::0::1' => '0008-12-05',
                '1::0::1' => '10000764005',
                '3::0::1' => '0211-11-01',
                '4::0::1' => '1967-06-25',
            )
        );

        //check 'release_date array isn't used if it's length isn't exactly 8
        $selectedReleaseData = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 0, $selectedReleaseData );

        $productData['release_date']['5::0::1'] = '19670101';

        //check 'release_date array is used if it's in YYYYMMDD format and a valid date
        $selectedReleaseData = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( $productData['release_date']['5::0::1'], $selectedReleaseData );
        $productData['release_date']['5::0::1'] = '19670000';

        //check 'release_date array is used if it's in YYYYMMDD format and a valid date(0000 should be changed to 0101)
        $selectedReleaseData = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( '19670101', $selectedReleaseData );

        $productData['release_date']['5::0::1'] = 'yyyymmdd';
        //check 'release_date array isn't used if it's in YYYYMMDD format but not  a valid date
        $selectedReleaseData = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 0, $selectedReleaseData );

        $productData['release_date']['5::0::1'] = '20153301';

        //check 'release_date array isn't used if it's in YYYYMMDD format but not  a valid date
        $selectedReleaseData = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 0, $selectedReleaseData );

        $productData = array(
            'release_date' => array(
                '0::0::1' => '14000101',
                '1::0::1' => '40000101',
            )
        );
        //check 'release_date array isn't used if it's in YYYYMMDD format but not in a valid range a valid date
        $selectedReleaseData = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 0, $selectedReleaseData );

        //if a validrelease dates
        //check product attributes release date

        //set test data
        $productData = array(
            'release_date'       => array(
                '0::0::1' => '19000101',//$releaseDateProducts[uk]
            ),
            'product_attributes' => array(
                'release_date' => array(
                    '0::0::1' => '17000102',//$releaseDateProducts[uk]
                    '1::0::1' => array('uk' => '20110101'),//$releaseDateDataProviderProducts[uk]
                    '2::0::1' => array(array('uk' => '20110202')),//$releaseDateDataProviderProducts[uk]
                    '3::0::1' => array(array(array('uk' => '20110201'))),//$releaseDateDataProviderProducts[other]
                )
            ),

        );
        //assert oldest date from $releaseDateDataProviderProducts[uk] is used
        $selectedReleaseDate = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 20110101, $selectedReleaseDate );

        //unset the used value
        unset( $productData['product_attributes']['release_date']['1::0::1'] );

        //assert the next oldest date from $releaseDateDataProviderProducts[uk] is used
        $selectedReleaseDate = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 20110202, $selectedReleaseDate );

        //unset the used value
        unset( $productData['product_attributes']['release_date']['2::0::1'] );

        //assert the next oldest date from $releaseDateDataProviderProducts[other] is used
        $selectedReleaseDate = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 20110201, $selectedReleaseDate );

        //unset the used value
        unset( $productData['product_attributes']['release_date']['3::0::1'] );

        //assert the oldest uk value from release date will be used next
        $selectedReleaseDate = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 17000102, $selectedReleaseDate );

        //unset the used value
        unset( $productData['product_attributes']['release_date']['0::0::1'] );

        //assert the oldest uk value from release date will be used next
        $selectedReleaseDate = $productAssemblerService->decideReleaseDate( $productData );
        $this->assertEquals( 19000101, $selectedReleaseDate );

        //unset the used value
        unset( $productData['release_date']['0::0::1'] );

    }

    /**
     * @covers ProductAssemblerService::decideProductAttributeArray()
     **/
    public function testDecideProductAttributeArray()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        $productAttributeArray = array('product_attributes' => array('attributeType' => array('00::01::00' => 'notValue', '01::01::01' => 'value')));
        //assert the dp product attribute is will be returned
        $attributeValue = $productAssemblerService->decideProductAttributeArray( $productAttributeArray, 1 );
        $this->assertEquals( array('attributeType' => 'value'), $attributeValue );

        $productAttributeArray = array('product_attributes' => array('attributeType' => array('00::01::00' => 'value', '00::01::02' => 'value')));
        //assert the product attribute is will be returned if no dps are set for it
        $attributeValue = $productAssemblerService->decideProductAttributeArray( $productAttributeArray, 1 );
        $this->assertEquals( array('attributeType' => 'value'), $attributeValue );

    }

    /**
     * @covers ProductAssemblerService::defaultDecideProductAttribute()
     **/
    public function testDefaultDecideProductAttribute()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        // if it's DPPIdentifier => value
        $productAttributeArray = array('00::01::00' => 'value');
        //assert when ignore product attribute is set nothing will be returned
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, true );
        $this->assertEquals( null, $attributeValue );

        //assert when ignore product attribute is set false the value will be returned
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, false );
        $this->assertEquals( 'value', $attributeValue );

        //assert json encoded array is returned for arrays
        $productAttributeArray = array('00::01::00' => array('key' => 'value'));
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, false );
        $this->assertEquals( json_encode( $productAttributeArray['00::01::00'] ), $attributeValue );

        //check ranking
        $productAttributeArray = array('00::01::01' => 'value', '00::01::02' => 'value', '00::01::03' => 'incorrectValue');
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, false );
        $this->assertEquals( 'value', $attributeValue );

        //subAttributeName => DPPIdentifier => value
        $productAttributeArray = array('productAttributeKey' => array('00::01::00' => 'value'));
        //assert when ignore product attribute is set nothing will be returned
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, true );
        $this->assertEquals( null, $attributeValue );

        //assert when ignore product attribute is set false the value will be returned
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, false );
        $this->assertEquals( json_encode( array('productAttributeKey' => 'value') ), $attributeValue );

        //check ranking
        $productAttributeArray = array('productAttributeKey' => array('00::01::01' => 'value', '00::01::02' => 'value', '00::01::03' => 'incorrectValue'));
        $attributeValue = $productAssemblerService->defaultDecideProductAttribute( $productAttributeArray, false );
        $this->assertEquals( json_encode( array('productAttributeKey' => 'value') ), $attributeValue );

    }

    /**
     * @covers ProductAssemblerService::updateMasterProductPeripheralInformation()
     **/
    public function testUpdateMasterProductPeripheralInformation()
    {
        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //generate send data
        $this->fixtureManager->load( array(
            'product_relation' => 'ProductRelation'
        ) );
        //get the product we want
        $masterProductId = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                ->getAttribute( 'product_id' );

        //delete  all master reviews, images, emcommerce links, and ratings
        UserReview::model()
                  ->deleteAllByAttributes( array('product_id' => $masterProductId) );
        ProductImage::model()
                    ->deleteAllByAttributes( array('product_id' => $masterProductId) );

        EcommerceProviderProduct::model()
                                ->deleteAllByAttributes( array('product_id' => $masterProductId) );
        UserRating::model()
                  ->deleteAllByAttributes( array('product_id' => $masterProductId) );
        UserReview::model()
                  ->deleteAllByAttributes( array('product_id' => $masterProductId) );

        //generate send data
        $this->fixtureManager->load( array(
            'product_relation'           => 'ProductRelation',
            'ecommerce_provider_product' => 'EcommerceProviderProduct',
            'category_product'           => 'CategoryProduct',
            //'product_image' => 'ProdutImage',
            'user_rating'                => 'UserRating',
            'user_review'                => 'UserReview'
        ) );

        $subordinateProducts = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttribute( 'related_product_id' ),
        );
        //send a product with no subs and assert null
        $this->assertNull( $productAssemblerService->updateMasterProductPeripheralInformation( 1, Product::model()
                                                                                                         ->getRootCategoryId( $masterProductId ) ) );

        //get all subordinate reviews, images, emcommerce links, and ratings
        $subordinateReviews = $subordinateEcommerce = $subordinateRatings = $subordinateImages = $subordinateCategories = array();

        foreach( $subordinateProducts as $productId )
        {
            //get master Categories
            $subordinateCategories = array_merge( UserReview::model()
                                                            ->findAllByAttributes( array('product_id' => $productId) ), $subordinateCategories );

            //get master images
            $subordinateImages = array_merge( ProductImage::model()
                                                          ->findAllByAttributes( array('product_id' => $productId) ), $subordinateImages );

            //get master ecommerce
            $subordinateEcommerce = array_merge( EcommerceProviderProduct::model()
                                                                         ->findAllByAttributes( array('product_id' => $productId) ), $subordinateEcommerce );

            //get master ratings
            $subordinateRatings = array_merge( UserRating::model()
                                                         ->findAllByAttributes( array('product_id' => $productId) ), $subordinateRatings );

            //get master reviews
            $subordinateReviews = array_merge( UserReview::model()
                                                         ->findAllByAttributes( array('product_id' => $productId) ), $subordinateReviews );

        }

        //compile all reviews and ratings by user id
        $subordinateReviewsByUserId = array();
        foreach( $subordinateReviews as $review )
        {
            $subordinateReviewsByUserId[$review->getAttribute( 'user_id' )] = $review->getAttributes();
        }
        $subordinateRatingsByUserId = array();
        foreach( $subordinateRatings as $rating )
        {
            $subordinateRatingsByUserId[$rating->getAttribute( 'user_id' )] = $rating->getAttributes();
        }

        //sort images by image image_url
        $subordinateImagesByImageUrl = array();
        foreach( $subordinateImages as $image )
        {
            $subordinateImagesByImageUrl[$image->getAttribute( 'image_url' )] = $image->getAttributes();
        }

        //sort ecommerce links by url
        $subordinateEcommerceByLink = array();
        foreach( $subordinateEcommerce as $ecommerce )
        {
            $subordinateEcommerceByLink[$ecommerce->getAttribute( 'ecommerce_link' )] = $ecommerce->getAttributes();
        }
        //update the master product peripheral info
        $productAssemblerService->updateMasterProductPeripheralInformation( $masterProductId, Product::model()
                                                                                                     ->getRootCategoryId( $masterProductId ) );

        //get master Categories
        $masterCategories = UserReview::model()
                                      ->findAllByAttributes( array('product_id' => $masterProductId) );

        //get master images
        $masterImages = ProductImage::model()
                                    ->findAllByAttributes( array('product_id' => $masterProductId) );

        //get master ecommerce
        $masterEcommerce = EcommerceProviderProduct::model()
                                                   ->findAllByAttributes( array('product_id' => $masterProductId) );

        //get master ratings
        $masterRatings = UserRating::model()
                                   ->findAllByAttributes( array('product_id' => $masterProductId) );

        //get master reviews
        $masterReviews = UserReview::model()
                                   ->findAllByAttributes( array('product_id' => $masterProductId) );
        //sort info as in subs
        $masterReviewsByUserId = array();
        foreach( $masterReviews as $review )
        {
            $masterReviewsByUserId[$review->getAttribute( 'user_id' )] = $review->getAttributes();
        };

        $masterRatingsByUserId = array();
        foreach( $masterRatings as $rating )
        {
            $masterRatingsByUserId[$rating->getAttribute( 'user_id' )] = $rating->getAttributes();
        };

        $masterImagesByImageUrl = array();
        foreach( $masterImages as $image )
        {
            $masterImagesByImageUrl[$image->getAttribute( 'image_url' )] = $image->getAttributes();
        }

        $masterEcommerceByLink = array();
        foreach( $masterEcommerce as $ecommerce )
        {
            $masterEcommerceByLink[$ecommerce->getAttribute( 'ecommerce_link' )] = $ecommerce->getAttributes();
        }

        //assert all subordinate review were added to the product
        foreach( $subordinateReviewsByUserId as $userId => $review )
        {
            $comparableAttributes = array(
                'review_note',
                "user_id",
                "last_updated",
                "num_comments",
                "num_likes",
                "num_dislikes",
                "review_date"
            );
            foreach( $comparableAttributes as $attributeName )
            {
                if( $attributeName == 'review_note' )
                {
                    $review[$attributeName] = preg_replace( "#\[iLink\s*name='.*\]#", '', $review[$attributeName] );
                    $masterReviewsByUserId[$userId][$attributeName] = preg_replace( "#\[iLink\s*name='.*\]#", '', $masterReviewsByUserId[$userId][$attributeName] );
                }
                $this->assertEquals( $review[$attributeName], $masterReviewsByUserId[$userId][$attributeName] );
            }
        }
        foreach( $subordinateRatingsByUserId as $userId => $rating )
        {
            $comparableAttributes = array(
                'rating',
                "last_updated",
            );
            foreach( $comparableAttributes as $attributeName )
            {

                $this->assertEquals( $rating[$attributeName], $masterRatingsByUserId[$userId][$attributeName] );
            }

        }

        foreach( $subordinateImagesByImageUrl as $imageUrl => $image )
        {
            $comparableAttributes = array(
                'image_url',
                'image_height',
                'image_width',
                'image_type',
            );
            foreach( $comparableAttributes as $attributeName )
            {
                if( $image[$attributeName] != $masterImagesByImageUrl[$imageUrl][$attributeName] )
                {
                    $productAssemblerService->debugMsg( $attributeName . ' - ' . $image[$attributeName] . ' - ' . $masterImagesByImageUrl[$imageUrl][$attributeName] );
                }
                $this->assertEquals( $image[$attributeName], $masterImagesByImageUrl[$imageUrl][$attributeName] );
            }

        }

        foreach( $subordinateEcommerceByLink as $link => $ecommerce )
        {

            $comparableAttributes = array(
                'ecommerce_link',
                'country_code',
                'ecommerce_provider_id'
            );
            foreach( $comparableAttributes as $attributeName )
            {
                $this->assertEquals( $ecommerce[$attributeName], $masterEcommerceByLink[$link][$attributeName] );

            }
        }
    }

    /**
     * @covers ProductAssemblerService::getValidSubordinateProductIds()
     **/
    public function testGetValidSubordinateProductIds()
    {

        //attempt results with duplication dp
        $productDuplicationDataProvider = new ProductDuplicationDataProvider();
        $productDuplicationDataProvider->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationDataProvider );
        $productAssemblerService->init( 'GB' );

        //generate send data
        $this->fixtureManager->load( array('data_provider_product' => 'DataProviderProduct') );

        //get the expected response
        $usedRecord = $this->fixtureManager->getRecord( 'data_provider_product', 'sample_4' );
        $expectedResult = json_decode( $usedRecord->getAttribute( 'product_attributes' ), true );
        $expectedResult = $expectedResult[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];

        $result = $productAssemblerService->getValidSubordinateProductIds( $usedRecord->getAttribute( 'product_id' ) );
        $this->assertEquals( sort( $expectedResult ), sort( $result ) );

        //attempt results with duplication service
        $productDuplicationService = new ProductDuplicationService();
        $productDuplicationService->init( 'GB' );
        $productAssemblerService = new ProductAssemblerService( $productDuplicationService );
        $productAssemblerService->init( 'GB' );

        //generate send data
        $this->fixtureManager->load( array('product_relation' => 'ProductRelation') );
        //get the product we want
        $masterProductId = $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                                ->getAttribute( 'product_id' );

        $result = $productAssemblerService->getValidSubordinateProductIds( $masterProductId );
        $expectedResult = array(
            $this->fixtureManager->getRecord( 'product_relation', 'sample_2' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_3' )
                                 ->getAttribute( 'related_product_id' ),
            $this->fixtureManager->getRecord( 'product_relation', 'sample_4' )
                                 ->getAttribute( 'related_product_id' ),
        );
        $this->assertEquals( sort( $expectedResult ), sort( $result ) );

    }


}