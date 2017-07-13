<?php

/**
 */
class ProductAssemblerService extends CApplicationComponent
{
    use ServiceTrait;

    const PRODUCT_CACHE_QUEUE_NAME = 'y_msg_queue_product_cache';
    const PRODUCT_ASSEMBLER_QUEUE_NAME = 'y_msg_queue_product_assembler';
    const SEARCH_ADD_QUEUE_NAME = 'y_msg_queue_search_add';
    const ADMIN_USER_DATA_PROVIDER_NAME = 'adminUser';

    const SOURCE_IDENTIFIER_DELIMITER = '::';

    public $productNameRegexPatterns = array(
        "/by\s*.*?\s*\(Author\)\s*on.*/i",
        "/(\s*\([^\(\)\[\]]*(?:(?1)[^\(\)\[\]]*)*+\)|\s*\[[^\(\)\[\]]*(?:(?1)[^\(\)\[\]]*)*+\])+$/", // matches brackets and square brackets. Also is recursive.
    );

    public $productNameRegexReplacements = array('', '');

    /**
     * @todo check values mapping, some dont match
     */
    protected $_attributeMappings = array(
        'NumberOfPages'   => 'pages',
        'PublicationDate' => 'published_date',
        'ISBN'            => 'isbn',
        'EISBN'           => 'eisbn',
        'urls'            => 'urls',
        'news'            => 'news',
        'release_date'    => 'release_date',
        'publishers'      => 'publisher',
        'developers'      => 'developer',
        'publishedDate'   => 'published_date',
        'related user'    => 'related_user',
        'relatedUser'     => 'related_user',
        'publisher'       => 'publisher',
        'pages'           => 'pages',
        'isbn'            => 'isbn',
        'format'          => 'bookFormat',
        'pageCount'       => 'pages',
        'ISBN_10'         => 'isbn',
        'ISBN_13'         => 'eisbn',
        'searchInfo'      => 'searchdata',
        'author'          => 'author',
        'release_dates'   => 'release_date',
        'releaseDate'         => 'releaseDate',
        'tracks'              => 'tracks',
        'Genres'              => 'genre',
        'genres'              => 'genre',
        'info'                => 'info',
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

    private $_dataProviderIdsArray;

    /**
     * @var MessageQueue
     */
    private $_messageQueue;

    /**
     * @var ProductService
     */
    private $_productService;

    /** @var CategoryProductService */
    private $_categoryProductService;

    /**
     * @var TextService
     */
    private $_textService;

    /**
     * @var ProductDuplicationDataProvider
     */
    private $_productDuplicationDataProvider;

    /**
     * @var TermExtractionService
     */
    private $_termExtractionService;

    private $_attributeTypeIds = array();

    private $variousArtistsProductId = 5248256;

    public function __construct( $productDuplicationDataProvider = null )
    {
        // get the default YII cache object and db connection
        $this->_db = YiiItcher::app()->db;
        $this->_cache = YiiItcher::app()->cache;
        $this->_readCache = YiiItcher::app()->readCache;
        $this->_dynamoDb = new A2DynamoDb();

        $this->_messageQueue = new MessageQueue();
        $this->_productService = new ProductService();
        $this->_categoryProductService = new CategoryProductService();
        $this->_textService = new TextService();
        $this->_termExtractionService = TermExtractionService::create();

        // If passed a product duplication data provider use that
        if( isset( $productDuplicationDataProvider ) && ( is_a( $productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) || is_a( $productDuplicationDataProvider, 'ProductDuplicationService' ) ) )
        {
            $this->_productDuplicationDataProvider = $productDuplicationDataProvider;
        }
        // if not get a new one
        else
        {
            $this->_productDuplicationDataProvider = new ProductDuplicationService();
            $this->_productDuplicationDataProvider->init( 'GB', $this );
        }

        // if we've been passed a debug buffer, associate it
        if( isset( YiiItcher::app()->params['apiDebugging'] ) )
        {
            $this->setDebug( self::$_DEBUG_LEVEL_1 );
            $this->setDebugBuffering();
            $this->startTimer( 'AssemblerService' );
        }

        $this->setDataProviderIdsArray();
    }

    public function setDebug( $debug = true, $recursive = true )
    {
        if( $recursive && $this->_productDuplicationDataProvider INSTANCEOF ProductDuplicationDataProvider )
        {
            $this->_productDuplicationDataProvider->setDebug( $debug, false );
        }
        $this->_productService->setDebug( $debug );
        $this->_termExtractionService->setDebug( $debug );
        $this->_debug = $debug;
    }

    /**
     * function sets $_dataProviderIdsArray. If passed an array it uses that, if not it tries though the cache then looks in the database.
     *
     * @param array $dataProviderIdsArray
     */
    private function setDataProviderIdsArray( array $dataProviderIdsArray = null )
    {
        if( !empty( $dataProviderIdsArray ) )
        {
            // if passed an array set to that
            $this->_dataProviderIdsArray = $dataProviderIdsArray;
        }
        else
        {
            // try the cache
            $cacheKey = "GenericService::DataProviderIdsArray";
            if( !$this->_dataProviderIdsArray = $this->cacheGet( $cacheKey ) )
            {
                // if the wasn't a result from the cache get it from the database
                $this->_dataProviderIdsArray = array();

                $dataProviderIdsSQL = "SELECT data_provider_name, data_provider_id FROM data_provider;";

                $dataProviderIdsResult = $this->_db->createCommand( $dataProviderIdsSQL )
                                                   ->queryAll();

                $this->_dataProviderIdsArray = array_column( $dataProviderIdsResult, 'data_provider_id', 'data_provider_name' );

                // then update the cache
                $this->cacheSet( $cacheKey, $this->_dataProviderIdsArray, self::$_CACHE_TIME_ONE_DAY );
            }
        }
    }

    /**
     * Runs or queues a product set by product id.
     *
     * @param int  $productId
     * @param bool $queueOnly
     *
     * @return bool
     */
    public function assembleProductByProductId( $productId, $queueOnly = false )
    {
        // construct params
        $params = array('product_id' => $productId);

        // call the actual function
        return $this->assembleProductByProductIdQueued( $params, $queueOnly );
    }

    /**
     * Controlling function for assembling a product.
     *
     * @param array $params
     * @param bool  $queueOnly
     *
     * @return bool - valid product
     */
    public function assembleProductByProductIdQueued( array $params, $queueOnly = false )
    {
        /*    decode params   */
        // we need a product id
        $assemblerProductId = $params['product_id'];
        if( !( is_scalar( $assemblerProductId ) && ctype_digit( (string)$assemblerProductId ) ) )
        {
            // if the product id isn't a plain number them bail
            return true;
        }

        // if set to queue only do so then return
        if( $queueOnly )
        {
            // queue to the assembler
            $this->queueMessage( $assemblerProductId );

            return true;
        }

        $this->startTimer( "\nStarting assembler for product id : " . $assemblerProductId );

        // find the root category of the supplied product (this will fail if it's an invalid product id or if the product is in no valid categories
        $rootCategorySQL = "SELECT c.root_category_id FROM category_product cp JOIN category c ON cp.category_id = c.category_id
            WHERE cp.product_id = :product_id AND c.root_category_id IN (" . $this->getRootCategoriesCSV( 'GB' ) . ") GROUP BY cp.product_id;";

        $assemblerProductRootCategoryId = $this->_db->createCommand( $rootCategorySQL )
                                                    ->queryScalar(
                                                        array(
                                                            ':product_id' => $assemblerProductId,
                                                        )
                                                    );

        if( empty( $assemblerProductRootCategoryId ) )
        {
            // if the product doesn't exist
            if( !$this->_db->createCommand( "SELECT TRUE FROM product WHERE product_id = :productId" )
                           ->queryScalar( array(':productId' => $assemblerProductId) )
            )
            {
                $this->debugMessage( 'no product for product id : ' . $assemblerProductId );

                // return true to remove it from the queue
                return true;
            }
            // if no valid category was found then bail
            $this->debugMessage( "No root category found for product id : $assemblerProductId" );

            // attempt to queue the product to the amazon data provider
            return $this->handleProductWithoutRootCategory( $assemblerProductId );
        }
        else
        {
            $this->debugMessage( 'Found root category id : ' . $assemblerProductRootCategoryId );
        }

        // if no_duplication_queue is set in the params use that (this will stop it from queueing to ProductDuplicationDataProvider)
        if( !empty( $params['no_duplication_queue'] ) )
        {
            $noDuplicationQueue = $params['no_duplication_queue'];
        }
        else
        {
            $noDuplicationQueue = false;
        }
        $rejectedAttributes = array();

        // get the products and data_provider_products for this product
        $validDataArray = $this->getAssemblerProductInformation( $assemblerProductId, $assemblerProductRootCategoryId );

        // if we have a valid product
        if( !empty( $validDataArray ) )
        {
            $rawProduct = $this->processRawProductData( $validDataArray, $rejectedAttributes );

            $formattedRawProduct = $this->formatRawProductData( $rawProduct );

            // assemble the final product from the collected data
            $assemblerProductProcessed = $this->assembleProduct( $formattedRawProduct, $assemblerProductId, $assemblerProductRootCategoryId/*, $rejectedAttributes*/ );

            // if the product is a master
            if( ( $assemblerProductRootCategoryId == self::booksRootId() or $assemblerProductRootCategoryId == self::gamesRootId() )
                && $this->_productDuplicationDataProvider->isMasterProduct( $assemblerProductId )
            )
            {
                // update its peripheral information (categories and images and the like)
                $this->updateMasterProductPeripheralInformation( $assemblerProductId, $assemblerProductRootCategoryId );
            }

            // save the product
            if( $this->saveAssembledProduct( $assemblerProductId, $assemblerProductProcessed ) )
            {
                // if any changes were made
                // update product terms and similar items
                if( $this->_termExtractionService->updateProductTerms( $assemblerProductId, $assemblerProductRootCategoryId, $assemblerProductProcessed['is_subproduct'] ) )
                {
                    // if any terms were changed.
                    $this->_categoryProductService->updateRelevanceForProduct( $assemblerProductId );
                    $this->_productService->updateProductSimilarProducts( $assemblerProductId );
                }

                #    // check for duplicates and handle them
                #    $duplicateManagementService = new PrototypeDuplicateManagementService();
                #    $duplicateManagementService->checkDuplicates( $assemblerProductId, $assemblerProductRootCategoryId, $assemblerProductProcessed );

                // queue the product to be indexed (or deleted from the index if it's a duplicate) and cleared from the cache
                $this->_messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $assemblerProductId, true );

                // unless set not to, if it's a book or a game then queue it to duplication detection
                if( !$noDuplicationQueue && ( $assemblerProductRootCategoryId == self::booksRootId() or $assemblerProductRootCategoryId == self::gamesRootId() ) )
                {
                    $this->_productDuplicationDataProvider->updateProduct( $assemblerProductId, true );
                }
            }
            $this->endTimer( "Finished Assembler for product : $assemblerProductId\n\n" );

            return true;
        }
        else
        {
            return false;
        }

    }

    private function queueMessage( $productId )
    {
        $this->_messageQueue->queueMessage( self::PRODUCT_ASSEMBLER_QUEUE_NAME, $productId );
    }

    private function handleProductWithoutRootCategory( $assemblerProductId, $ignoreProductDuplicationDataProvider = false )
    {
        if( $ignoreProductDuplicationDataProvider )
        {
            $dataProvidersSQL = "SELECT data_provider_id FROM data_provider_product WHERE product_id = :productId AND NOT data_provider_id = :productDuplicationDataProviderId;";

            $dataProvidersArray = $this->_db->createCommand( $dataProvidersSQL )
                                            ->queryColumn(
                                                array(
                                                    ':productId'                        => $assemblerProductId,
                                                    ':productDuplicationDataProviderId' => $this->_dataProviderIdsArray[ProductDuplicationDataProvider::PRODUCT_DUPLICATION_PROVIDER_NAME]
                                                )
                                            );
        }
        else
        {
            $dataProvidersSQL = "SELECT data_provider_id FROM data_provider_product WHERE product_id = :productId;";

            $dataProvidersArray = $this->_db->createCommand( $dataProvidersSQL )
                                            ->queryColumn( array(':productId' => $assemblerProductId) );
        }

        if( empty( $dataProvidersArray ) && is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) )
        {
            $this->debugMessage( 'product has no data providers, deleting' );
            // it's a product with no data providers, so delete it
            $this->deleteProduct( $assemblerProductId );

            return true;
        }
        elseif( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationService' ) )
        {

            // detect the type of product it is
            $productRelations = $this->_productDuplicationDataProvider->getProductRelations( $assemblerProductId );

            // if it's a not master product
            if( isset( $productRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
            {
                // if it's a master product
                // get its relationships
                $this->debugMessage( 'getting RootCategoryId from subordinates, no de-queue' );
                // add it to the root category of the subordinates
                $commonRootCategorySQL = "SELECT c.root_category_id FROM category c JOIN category_product cp ON c.category_id=cp.category_id WHERE cp.product_id IN (" . implode( ',', $productRelations[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) . ")
                    AND c.root_category_id IN(" . $this->getRootCategoriesCSV( 'GB' ) . ") GROUP BY c.root_category_id ORDER BY COUNT(1) DESC;";
                $commonRootCategory = $this->_db->createCommand( $commonRootCategorySQL )
                                                ->queryScalar();

                $this->debugMsg( 'setting ' . $commonRootCategory . ' as root category for ' . $assemblerProductId );
                $categoryProduct = new CategoryProduct;
                $categoryProduct->setAttributes( array(
                    'product_id'  => $assemblerProductId,
                    'category_id' => $commonRootCategory
                ) );
                $categoryProduct->save();

                // don't allow it to de-queue
                return false;

            }
            elseif( empty( $dataProvidersArray ) && ( isset( $productRelations[ProductDuplicationDataProvider::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) || isset( $productRelations[ProductDuplicationDataProvider::CONFLICTED_MASTER_PRODUCT_IDS_KEY] ) || isset( $productRelations[ProductDuplicationDataProvider::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] ) ) )
            {
                // test it again with the PDDPP excluded
                $this->debugMsg( 'product is not a master but has dps, carrying on' );

            }
            else
            {
                $this->debugMessage( 'product is not a master, or dataproviders, deleting' );
                $this->deleteProduct( $assemblerProductId );

                // allow it to de-queue
                return true;

            }

        }

        // if it has a productDuplication data provider product

        if( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) )
        {
        if( in_array( $this->_dataProviderIdsArray[ProductDuplicationDataProvider::PRODUCT_DUPLICATION_PROVIDER_NAME], $dataProvidersArray ) )
        {
            $this->debugMessage( 'product has product duplication data provider' );

            // handle it
            return $this->handleProductWithoutRootCategoryWithProductDuplicationDataProvider( $assemblerProductId );
        }
        }

        // if it has a amazon data provider product
        if( in_array( $this->_dataProviderIdsArray[AmazonDataProvider::AMAZON_DATA_PROVIDER_NAME], $dataProvidersArray ) )
        {
            $this->debugMessage( 'product has amazon data provider' );

            // handle it
            return $this->handleProductWithoutRootCategoryWithAmazonDataProvider( $assemblerProductId );
        }

        // if it has a Discogs data provider product
        if( in_array( $this->_dataProviderIdsArray[DiscogsDataProvider::DISCOGS_DATA_PROVIDER_NAME], $dataProvidersArray ) )
        {
            $this->debugMessage( 'product has discogs data provider' );

            // handle it
            return $this->handleProductWithoutRootCategoryWithDiscogsDataProvider( $assemblerProductId );
        }

        // if it has a TMDB data provider product
        if( in_array( $this->_dataProviderIdsArray[TMDbDataProvider::TMDB_DATA_PROVIDER_NAME], $dataProvidersArray ) )
        {
            $this->debugMessage( 'product has TMDB data provider' );

            // handle it
            return $this->handleProductWithoutRootCategoryWithTMDBDataProvider( $assemblerProductId );
        }

        // if it has a theGamesDatabase data provider product
        if( in_array( $this->_dataProviderIdsArray[TheGamesDBDataProvider::THE_GAMES_DB_DATA_PROVIDER_NAME], $dataProvidersArray ) )
        {
            $this->debugMessage( 'product has theGamesDatabase data provider' );

            // handle it
            return false;
        }

        $this->debugMessage( 'product has no top-level data providers' );

        // if it has only a rotten tomatoes data provider
        if( $dataProvidersArray == array($this->_dataProviderIdsArray[RottenTomatoesDataProvider::RT_DATA_PROVIDER_NAME]) )
        {
            // delete it
            $this->debugMessage( 'product has only Rotten Tomatoes data provider, deleting' );
            $this->deleteProduct( $assemblerProductId );

            // de-queue
            return true;
        }

        // if it haa only a good reads data provider
        if( $dataProvidersArray == array($this->_dataProviderIdsArray[GoodReadsDataProvider::GOOD_READS_DATA_PROVIDER_NAME]) )
        {
            $this->debugMessage( 'product has only Good Reads data provider' );

            // if it's set to display
            if( $this->_db->createCommand( "SELECT display FROM product WHERE product_id = :productId" )
                          ->queryScalar( array(':productId' => $assemblerProductId) )
            )
            {
                // set it not to
                $this->debugMessage( 'setting display to 0' );

                /** @var Product $product */
                $product = Product::model()
                                  ->findByPk( $assemblerProductId );
                $product->display = 0;
                $product->save();

                $this->_messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $assemblerProductId, true );
            }

            // de-queue it
            return true;
        }
        $this->debugMessage( print_r( $dataProvidersArray, true ) );

        return false;
    }

    /**
     * Function deletes a product then queues its removal for the cache and search index
     *
     * @param Int $productId
     *
     * @throws Exception
     */
    protected function deleteProduct( $productId )
    {
        Utilities::deleteProduct( $productId );

        $this->_messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $productId, true );
    }

    /**
     * Handles a product with no categories but with a product duplication data provider
     *
     * @param $assemblerProductId
     *
     * @return bool
     */
    private function handleProductWithoutRootCategoryWithProductDuplicationDataProvider( $assemblerProductId )
    {
        if( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) )
        {
            // detect the type of product it is
            $productTypeSQL = "SELECT product_name FROM data_provider_product WHERE product_id = :productId AND data_provider_id = :dataProviderId";
            $productType = $this->_db->createCommand( $productTypeSQL )
                                     ->queryScalar( array(
                                         ':productId'      => $assemblerProductId,
                                         ':dataProviderId' => $this->_dataProviderIdsArray[ProductDuplicationDataProvider::PRODUCT_DUPLICATION_PROVIDER_NAME]
                                     ) );

            switch( $productType )
            {
                // if it's a not master product
                case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                case ProductDuplicationDataProvider::CONFLICTED_PRODUCT_NAME:
                case ProductDuplicationDataProvider::UNIQUE_PRODUCT_NAME:
                    // test it again with the PDDPP excluded
                    $this->debugMessage( 'product is not a master, excluding PDDPP' );

                    return $this->handleProductWithoutRootCategory( $assemblerProductId, true );
                // if it's a master product
                case ProductDuplicationDataProvider::MASTER_PRODUCT_NAME:
                    // get its relationships
                    $this->debugMessage( 'product is a master' );
                    $productRelationshipsSQL = "SELECT product_attributes FROM data_provider_product WHERE product_id = :productId AND data_provider_id = :dataProviderId";
                    $productRelationshipsArray = json_decode( $this->_db->createCommand( $productRelationshipsSQL )
                                                                        ->queryScalar( array(
                                                                            ':productId'      => $assemblerProductId,
                                                                            ':dataProviderId' => $this->_dataProviderIdsArray[ProductDuplicationDataProvider::PRODUCT_DUPLICATION_PROVIDER_NAME]
                                                                        ) ), true );

                    return $this->handleProductWithoutRootCategory( $assemblerProductId, true );
                // if it's a master product
                case ProductDuplicationDataProvider::MASTER_PRODUCT_NAME:
                    // get its relationships
                    $this->debugMessage( 'product is a master' );
                    $productRelationshipsSQL = "SELECT product_attributes FROM data_provider_product WHERE product_id = :productId AND data_provider_id = :dataProviderId";
                    $productRelationshipsArray = json_decode( $this->_db->createCommand( $productRelationshipsSQL )
                                                                        ->queryScalar( array(
                                                                            ':productId'      => $assemblerProductId,
                                                                            ':dataProviderId' => $this->_dataProviderIdsArray[ProductDuplicationDataProvider::PRODUCT_DUPLICATION_PROVIDER_NAME]
                                                                        ) ), true );

                    // if it has no subordinates
                    if( empty( $productRelationshipsArray[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
                    {
                        $this->debugMessage( 'product has no subordinates, deleting' );
                        // delete it
                        $this->deleteProduct( $assemblerProductId );

                        // allow it to de-queue
                        return true;
                    }
                    else
                    {
                        $this->debugMessage( 'getting RootCategoryId from subordinates, no de-queue' );
                        // add it to the root category of the subordinates
                        $commonRootCategorySQL = "SELECT c.root_category_id FROM category c JOIN category_product cp ON c.category_id=cp.category_id WHERE cp.product_id IN (" . implode( ',', $productRelationshipsArray[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) . ")
                    AND c.root_category_id IN(" . $this->getRootCategoriesCSV( 'GB' ) . ") GROUP BY c.root_category_id ORDER BY COUNT(1) DESC;";
                        $commonRootCategory = $this->_db->createCommand( $commonRootCategorySQL )
                                                        ->queryScalar();
                        $categoryProduct = new CategoryProduct;
                        $categoryProduct->setAttributes( array(
                            'product_id'  => $assemblerProductId,
                            'category_id' => $commonRootCategory
                        ) );
                        $categoryProduct->save();

                        // don't allow it to de-queue
                        return false;
                    }

            }
        }

        return false;
    }


    /**
     * function tries to queue a product to the amazon data provider and returns true on success
     *
     * @param $assemblerProductId
     *
     * @return bool
     */
    private function handleProductWithoutRootCategoryWithAmazonDataProvider( $assemblerProductId )
    {
        // SQL query to identify kindle products
        $kindleSQL = "SELECT TRUE FROM category_product cp JOIN category c ON cp.category_id = c.category_id WHERE cp.product_id = :productId AND c.root_category_id = :rootCategoryId;";

        // if the product is a kindle product
        if( $this->_db->createCommand( $kindleSQL )
                      ->queryScalar( array(':productId' => $assemblerProductId, ':rootCategoryId' => Category::getIdFromName( 'Kindle Store' )) )
        )
        {
            // return true (de queue the product)
            $this->debugMessage( "product is a Kindle Store product" );

            return true;
        }

        // sql query to find asin
        $asinSQL = "SELECT data_provider_product_id FROM data_provider_product WHERE product_id = :productId AND data_provider_id = :dataProviderId";

        // get and initialise an amazon data provider
        $amazonDataProvider = new AmazonDataProvider();
        $amazonDataProvider->init( 'GB' );

        // execute the query
        // on success
        if( $asin = $this->_db->createCommand( $asinSQL )
                              ->queryScalar(
                                  array(
                                      ':productId'      => $assemblerProductId,
                                      ':dataProviderId' => $this->_dataProviderIdsArray[AmazonDataProvider::AMAZON_DATA_PROVIDER_NAME]
                                  )
                              )
        )
        {
            // queue the product to the amazon data provider
            $amazonDataProvider->getAsinsQueued( array('asins' => $asin, 'product_id' => $assemblerProductId), true );
            $this->debugMessage( "queued product to amazon data provider" );

            return true;
        }
        // if no asin was found (should never happen)
        else
        {
            $this->debugMessage( "no asin found either, no de-queue" );

            return false;
        }
    }

    /**
     * function handles discogs (music) products with no categories
     *
     * @param int $assemblerProductId
     *
     * @return bool
     */
    private function handleProductWithoutRootCategoryWithDiscogsDataProvider( $assemblerProductId )
    {
        // get the subproducts
        $subproductIdsSQL = "SELECT subproduct_id FROM product_subproduct WHERE product_id = :productId;";
        $subproductIdsArray = $this->_db->createCommand( $subproductIdsSQL )
                                        ->queryColumn( array(':productId' => $assemblerProductId) );

        // if it has no subproducts
        if( empty( $subproductIdsArray ) )
        {
            // and it isn't a subproduct
            $isSubproductSQL = "SELECT TRUE FROM product_subproduct WHERE subproduct_id = :productId;";
            if( !$this->_db->createCommand( $isSubproductSQL )
                           ->queryScalar( array(':productId' => $assemblerProductId) )
            )
            {
                $this->debugMessage( 'product has neither subproducts nor a parent product, deleting' );
                // delete it
                $this->deleteProduct( $assemblerProductId );

                return true;
            }
        }

        return false;
    }

    /**
     * Function queues the product for update via TMDB
     *
     * @param int $productId
     *
     * @return bool
     */
    private function handleProductWithoutRootCategoryWithTMDBDataProvider( $productId )
    {
        $tmdbDataProvider = new TMDbDataProvider();
        $tmdbDataProvider->init( 'GB' );

        $tmdbDataProvider->getMovieProductInfoQueued(
            array(
                'category_id'               => self::moviesRootId(),
                'product_id'                => $productId,
                'related_parent_product_id' => null,
                'relevancy_score'           => 0,
                'force_update'              => true
            ),
            true
        );

        return true;
    }

    /**
     * this function returns the useful, valid data about a given product ID, controlling what method is used to retrieve that information
     *
     * @param $assemblerProductId
     * @param $rootCategoryId
     *
     * @return array
     */
    public function getAssemblerProductInformation( $assemblerProductId, $rootCategoryId )
    {
        $validProductInformation = array();

        // decide the method to retrieve the data based on the root category
        switch( $rootCategoryId )
        {
            case self::musicRootId():
                // if it's a music product check if it is an artist or not (artists are not subproducts
                if( $this->isSubproduct( $assemblerProductId ) )
                {
                    // non artist music products get the generic handler
                    $assemblerProductInformation = $this->getIndividualProductInformation( $assemblerProductId );
                    if( empty( $assemblerProductInformation ) )
                    {
                        return null;
                    }
                    $validProductInformation = $this->individualProductInformationValidation( $assemblerProductInformation, $assemblerProductId );
                }
                else
                {
                    // artists get special handling
                    $assemblerProductInformation = $this->getMusicArtistProductInformation( $assemblerProductId );
                    if( empty( $assemblerProductInformation ) )
                    {
                        return null;
                    }
                    $validProductInformation = $this->musicArtistInformationValidation( $assemblerProductInformation, $assemblerProductId );

                }
                break;
            case self::moviesRootId():
            case self::tvShowsRootId():
                // all movies products use the generic handler
                $assemblerProductInformation = $this->getIndividualProductInformation( $assemblerProductId );
                if( empty( $assemblerProductInformation ) )
                {
                    return null;
                }
                $validProductInformation = $this->individualProductInformationValidation( $assemblerProductInformation, $assemblerProductId );
                break;
            case self::booksRootId():
            case self::gamesRootId():
                // if it's a book or game check if it's a master product
                if( $this->_productDuplicationDataProvider->isMasterProduct( $assemblerProductId ) )
                {
                    // if it is a master product use the special master handlers
                    $assemblerProductInformation = $this->getMasterProductInformation( $assemblerProductId );
                    if( empty( $assemblerProductInformation ) )
                    {
                        return null;
                    }
                    $validProductInformation = $this->masterProductInformationValidation( $assemblerProductInformation, $assemblerProductId );
                }
                else
                {
                    // non master products get the generic handlers
                    $assemblerProductInformation = $this->getIndividualProductInformation( $assemblerProductId );
                    if( empty( $assemblerProductInformation ) )
                    {
                        return null;
                    }
                    $validProductInformation = $this->individualProductInformationValidation( $assemblerProductInformation, $assemblerProductId );
                }
                break;

        }

        return $validProductInformation;
    }

    /**
     * function detects if a music product is an artist or not (artists are not sub products) Caches result for 1 hour
     *
     * @param $productId
     *
     * @return bool
     */
    public function isSubproduct( $productId )
    {
        $cacheKey = "ProductAssemblerService::IsSubproduct::$productId";

        if( !$strResult = $this->cacheGet( $cacheKey ) )
        {
            $isSubproduct = $this->_db->createCommand( "SELECT is_subproduct FROM product WHERE product_id = :productId;" )
                                      ->queryScalar(
                                          array(
                                              ':productId' => $productId
                                          )
                                      );

            $this->cacheSet(
                $cacheKey,
                ( $isSubproduct ?
                    'true' :
                    'false' ),
                self::$_CACHE_TIME_ONE_HOUR
            );

            return $isSubproduct ?
                true :
                false;
        }
        else
        {
            return ( $strResult === 'true' ) ?
                true :
                false;
        }
    }

    /**
     * This function gathers relevant data form the database about a product that isn't in the music root category.
     *
     * @param int $assemblerProductId
     *
     * @return array
     */
    public function getIndividualProductInformation( $assemblerProductId )
    {
        $this->debugMessage( "getting information about individual product : $assemblerProductId" );
        // get data directly about this product from the database
        $productsArray = array($this->getProductData( $assemblerProductId ));
        if( empty( $productsArray ) )
        {
            die( "No product found for product id : $assemblerProductId" );
        }
        // get the information about the current product attributes
        $productAttributesArray = $this->getProductAttributesArray( $assemblerProductId );
        // get the relevant dat_provider_products
        $dataProviderProductsArray = $this->getDataProviderProductsArray( $assemblerProductId );

        return array(
            'product'               => $productsArray,
            'product_attribute'     => $productAttributesArray,
            'data_provider_product' => $dataProviderProductsArray,
        );
    }

    /**
     * Gets the data that we process from the product table in the database, given a product id.
     *
     * @param int $productId
     *
     * @return CDbDataReader|mixed
     */
    public function getProductData( $productId )
    {
        $productSQL = "SELECT product_id, product_name, product_description, last_updated, release_date, is_subproduct FROM product WHERE product_id = :productId;";
        $this->debugMessage( $productSQL, 2 );

        $productData = YiiItcher::app()->db->createCommand( $productSQL )
                                           ->queryRow(
                                               true,
                                               array(
                                                   ':productId' => $productId
                                               )
                                           );

        if( $productData )
        {
            $this->debugMessage( "Found database product : $productId" );
            $this->debugMessage( json_encode( $productData ), 3 );
        }
        else
        {
            $this->debugMessage( "Failed to find database product : $productId" );
        }

        return $productData;
    }

    /**
     * Gets all current product attributes associated with a given product id.
     * if given a list of attribute type ids then it will limit to those
     *
     * @param int   $productId
     * @param array $productAttributesTypeIdsArray
     *
     * @return array|CDbDataReader
     */
    public function getProductAttributesArray( $productId, array $productAttributesTypeIdsArray = null )
    {
        $importedAttributes = $this->getImportedProductAttributesArray( $productId, $productAttributesTypeIdsArray );
        $adminAttributes = $this->getAdminProductAttributesArray( $productId, $productAttributesTypeIdsArray );
        $productAttributesArray = $this->cleanUpAttributes( $importedAttributes, $adminAttributes );

        return $productAttributesArray;
    }

    /**
     * Gets all current imported product attributes associated with a given product id.
     * if given a list of attribute type ids then it will limit to those
     *
     * @param int   $productId
     * @param array $productAttributesTypeIdsArray
     *
     * @return array|CDbDataReader
     */
    public function getImportedProductAttributesArray( $productId, array $productAttributesTypeIdsArray = null )
    {

        //check if adminUser overrides are set
        if( empty( $productAttributesTypeIdsArray ) )
        {
            $productAttributesSQL = "SELECT pa.id, pa.product_id, pa.product_attribute_value, pat.product_attribute_type_name FROM product_attribute pa
            JOIN product_attribute_type pat ON pa.product_attribute_type_id=pat.product_attribute_type_id WHERE pa.product_id = :productId;";
        }
        else
        {
            $productAttributesSQL = "SELECT pa.id, pa.product_id, pa.product_attribute_value, pat.product_attribute_type_name FROM product_attribute pa
            JOIN product_attribute_type pat ON pa.product_attribute_type_id=pat.product_attribute_type_id WHERE pa.product_id = :productId
            AND pa.product_attribute_type_id IN (" . implode( ',', $productAttributesTypeIdsArray ) . ");";
        }
        $this->debugMessage( $productAttributesSQL, 2 );
        $productAttributesArray = YiiItcher::app()->db->createCommand( $productAttributesSQL )
                                                      ->queryAll(
                                                          true,
                                                          array(
                                                              ':productId' => $productId
                                                          )
                                                      );
        $this->debugMessage( "Found " . count( $productAttributesArray ) . " product attributes for product : $productId" );
        $this->debugMessage( json_encode( $productAttributesArray ), 3 );

        return $productAttributesArray;
    }

    /**
     * Fetches productAttributes that were generated by an Admin User
     *
     * @param int        $productId
     * @param array|null $productAttributesTypeIdsArray
     *
     * @return array|CDbDataReader
     * @throws CDbException
     */
    public function getAdminProductAttributesArray( $productId, array $productAttributesTypeIdsArray = null )
    {
        if( empty( $productAttributesTypeIdsArray ) )
        {
            $productAttributesSQL = "SELECT pa.id, pa.product_id, pa.product_attribute_value, pat.product_attribute_type_name FROM product_attribute_admin_override pa
            JOIN product_attribute_type pat ON pa.product_attribute_type_id=pat.product_attribute_type_id WHERE pa.product_id = :productId;";
        }
        else
        {
            $productAttributesSQL = "SELECT pa.id, pa.product_id, pa.product_attribute_value, pat.product_attribute_type_name FROM product_attribute_admin_override pa
            JOIN product_attribute_type pat ON pa.product_attribute_type_id=pat.product_attribute_type_id WHERE pa.product_id = :productId
            AND pa.product_attribute_type_id IN (" . implode( ',', $productAttributesTypeIdsArray ) . ");";
        }
        $this->debugMessage( $productAttributesSQL, 2 );
        $productAttributesArray = YiiItcher::app()->db->createCommand( $productAttributesSQL )
                                                      ->queryAll( true, array(
                                                          ':productId' => $productId
                                                      ) );
        $this->debugMessage( "Found " . count( $productAttributesArray ) . " product attributes for product : $productId" );

        $this->debugMessage( json_encode( $productAttributesArray ), 3 );

        return $productAttributesArray;
    }

    private function cleanUpAttributes( $importedAttributes, $adminAttributes )
    {
        $orderedAdminAttributes = array();

        //order adminUser attributes by attribute type name
        foreach( $adminAttributes as $numericKey => $attributeData )
        {
            $orderedAdminAttributes[$attributeData['product_attribute_type_name']] = $attributeData;
        }
        //cycle through imported attributes array, if one is set by adminUser compile them
        foreach( $importedAttributes as $numericKey => $attributeData )
        {
            switch( $attributeData['product_attribute_type_name'] )
            {
                //these json attributes should get overwritten by the adminUser data
                case 'single_id':
                case 'album_id':
                case 'news':
                case 'video_reviews':
                case 'trailers':
                case 'cast':
                case 'crew':

                    //if adminUser data is set cycle through it for excluded items
                    if( isset( $orderedAdminAttributes[$attributeData['product_attribute_type_name']] ) )
                    {
                        $cleanAdminData = array();
                        //check if json, if not just use adminUser data
                        $jsonData = json_decode( $orderedAdminAttributes[$attributeData['product_attribute_type_name']]['product_attribute_value'], true );
                        if( $jsonData != false && $jsonData != null )
                        {
                            foreach( $jsonData as $values )
                            {
                                if( !isset( $values['exclude'] ) || $values['exclude'] != true )
                                {

                                    $cleanAdminData[] = $values;
                                }
                            }

                        }
                        else
                        {
                            $cleanAdminData = $orderedAdminAttributes[$attributeTypeName]['product_attribute_value'];
                        }
                        $importedAttributes[$numericKey]['product_attribute_value'] = json_encode( $cleanAdminData );
                    }
                    break;
            }
        }

        return $importedAttributes;
    }

    /**
     * Gets all data provider products associated with a given product id.
     * if given a list of data provider ids it will limit to those.
     *
     * @param int   $productId
     * @param array $dataProviderIdsArray
     *
     * @return array|CDbDataReader
     */
    public function getDataProviderProductsArray( $productId, array $dataProviderIdsArray = null )
    {
        if( empty( $dataProviderIdsArray ) )
        {
            $dataProviderProductSQL = "SELECT product_id, data_provider_id, data_provider_product_id,  product_name, product_description, product_attributes, last_updated
            FROM data_provider_product WHERE product_id =  :productId;";
        }
        else
        {
            $dataProviderProductSQL = "SELECT product_id, data_provider_id, data_provider_product_id,  product_name, product_description, product_attributes, last_updated
            FROM data_provider_product WHERE product_id =  :productId AND  data_provider_id IN (" . implode( ',', $dataProviderIdsArray ) . ");";
        }
        $this->debugMessage( $dataProviderProductSQL, 2 );
        $dataProviderProductsArray = YiiItcher::app()->db->createCommand( $dataProviderProductSQL )
                                                         ->queryAll(
                                                             true,
                                                             array(
                                                                 ':productId' => $productId
                                                             )
                                                         );
        $this->debugMessage( "Found " . count( $dataProviderProductsArray ) . " data provider products for product : $productId" );
        $this->debugMessage( json_encode( $dataProviderProductsArray ), 3 );

        return $dataProviderProductsArray;
    }

    /**
     * This function validates information retrieved for a product that is individual.
     * It keeps only information associated with the product's product_id
     *
     * @param array $productInformation
     * @param int   $productId
     *
     * @return array
     */
    public function individualProductInformationValidation( array $productInformation, $productId )
    {
        $this->debugMessage( "validating product information as an individual product" );
        $validProductInformation = array(
            'product'               => array(),
            'product_attribute'     => array(),
            'data_provider_product' => array(),
        );

        // check each product (there should only be one)
        foreach( $productInformation['product'] as $productKey => $product )
        {
            switch( $product['product_id'] )
            {
                case $productId:
                    // keep it if it's the right product
                    $this->debugMessage( "valid product : " . $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] ), 2 );
                    $validProductInformation['product'][$productKey] = $product;
                    break;
                default:
                    $this->debugMessage( "invalid product : " . $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] ), 2 );
                    break;
            }
        }

        // check each product attribute
        foreach( $productInformation['product_attribute'] as $productAttributeKey => $productAttribute )
        {
            switch( $productAttribute['product_id'] )
            {
                case $productId:
                    // built the capability to perform checks based on attribute type (but not implemented
                    switch( $productAttribute['product_attribute_type_name'] )
                    {
                        default:
                            // keep the attribute
                            $this->debugMessage( "valid product attribute : " . $this->constructDataProviderProductIdentifier( 0, 'attribute_id:' . $productAttribute['id'], $productAttribute['product_id'] ), 2 );
                            $validProductInformation['product_attribute'][$productAttributeKey] = $productAttribute;
                            break 2;
                    }
                default:
                    $this->debugMessage( "invalid product attribute : " . $this->constructDataProviderProductIdentifier( 0, 'attribute_id:' . $productAttribute['id'], $productAttribute['product_id'] ), 2 );
                    break;
            }
        }

        // check each data_provider_product
        foreach( $productInformation['data_provider_product'] as $dataProviderProductKey => $dataProviderProduct )
        {
            // check the product id
            switch( $dataProviderProduct['product_id'] )
            {
                case $productId:
                    // check the data provider id
                    switch( $dataProviderProduct['data_provider_id'] )
                    {
                        case $this->_dataProviderIdsArray['spotify']:
                        case $this->_dataProviderIdsArray['googleBooks']:
                        case $this->_dataProviderIdsArray['sevenDigital']:
                        case $this->_dataProviderIdsArray['fanArt']:
                        case $this->_dataProviderIdsArray['productDuplication']:
                            // discard certain data providers
                            $this->debugMessage( "invalid data provider product : " . $this->constructDataProviderProductIdentifier( $dataProviderProduct['data_provider_id'], $dataProviderProduct['data_provider_product_id'], $dataProviderProduct['product_id'] ), 2 );
                            break 2;
                        default:
                            // keep the rest
                            $this->debugMessage( "valid data provider product : " . $this->constructDataProviderProductIdentifier( $dataProviderProduct['data_provider_id'], $dataProviderProduct['data_provider_product_id'], $dataProviderProduct['product_id'] ), 2 );
                            $validProductInformation['data_provider_product'][$dataProviderProductKey] = $dataProviderProduct;
                            break 2;
                    }
                default:
                    $this->debugMessage( "invalid data provider product : " . $this->constructDataProviderProductIdentifier( $dataProviderProduct['data_provider_id'], $dataProviderProduct['data_provider_product_id'], $dataProviderProduct['product_id'] ), 2 );
                    break;
            }
        }

        return $validProductInformation;
    }

    /**
     * function creates a string that acts as a unique identifier for a data provider product and contains more useful and readable information than just the dataProviderProductId
     *
     * @param $dataProviderId
     * @param $dataProviderProductId
     * @param $productId
     *
     * @return string
     */
    private function constructDataProviderProductIdentifier( $dataProviderId, $dataProviderProductId, $productId )
    {
        return $dataProviderId . self::SOURCE_IDENTIFIER_DELIMITER . $dataProviderProductId . self::SOURCE_IDENTIFIER_DELIMITER . $productId;
    }

    /**
     * This function gathers and formats all relevant data about a product in the music root category.
     *
     * @param int $assemblerProductId
     *
     * @return array
     */
    public function getMusicArtistProductInformation( $assemblerProductId )
    {
        $this->debugMessage( 'Find saved data for music product' );

        // get data directly about this product from the database
        $productsArray = array($this->getProductData( $assemblerProductId ));
        if( empty( $productsArray ) )
        {
            die( "No product found for product id : $assemblerProductId" );
        }
        $productAttributesArray = $this->getProductAttributesArray( $assemblerProductId );
        $dataProviderProductsArray = $this->getDataProviderProductsArray( $assemblerProductId );

        // find products that share a name with this one
        $commonNamedProductIds = $this->listArtistsByNameOrderByPopularity( $productsArray[0]['product_name'] );

        // see if we found an echoNest DPP
        $hasEchoNestDataProviderProduct = false;
        foreach( $dataProviderProductsArray as $dataProviderProduct )
        {
            if( $dataProviderProduct['data_provider_id'] == $this->_dataProviderIdsArray['echoNest'] )
            {
                $hasEchoNestDataProviderProduct = true;
                break;
            }
        }

        switch( ( ( $commonNamedProductIds[0] == $assemblerProductId ) ? 't' : 'f' ) . ( $hasEchoNestDataProviderProduct ? 't' : 'f' ) )
        {
            case 'tt':
                // if this is the most popular product and has an echoNest DPP already
                $this->debugMessage( "Product is most popular and has echoNest data provider product" );
                // make sure it isn't set as archived
                $this->markProductNotArchivedByProductId( $assemblerProductId );
                break;

            case 'tf':
                // if this is the most popular product but does not have an echoNest DPP
                $this->debugMessage( "Product is most popular and needs echoNest data provider product" );

                // look for a less popular product with the same name that does have one
                $echoNestProductIdSQL = "SELECT product_id FROM data_provider_product WHERE product_id IN (" . implode( ',', $commonNamedProductIds ) . ") AND data_provider_id = :dataProviderId;";
                $echoNestProductId = $this->_db->createCommand( $echoNestProductIdSQL )
                                               ->queryScalar( array(
                                                       ':dataProviderId' => $this->_dataProviderIdsArray['echoNest']
                                                   ) );

                if( $echoNestProductId )
                {
                    $this->debugMessage( "Found product with echoNest provider : " . $echoNestProductId );
                    $nonDiscogsDataProviderIdsArray = $this->_dataProviderIdsArray;
                    unset( $nonDiscogsDataProviderIdsArray['discogs'] );
                    // use all of that product's DPPs (except for discogs) under the assumption that they should be for the more popular product
                    $dataProviderProductsArray = array_merge( $dataProviderProductsArray, $this->getDataProviderProductsArray( $echoNestProductId, $nonDiscogsDataProviderIdsArray ) );

                    // mark that product as archived
                    $this->markProductArchivedByProductId( $echoNestProductId );
                }
                // make sure this product isn't marked as archived
                $this->markProductNotArchivedByProductId( $assemblerProductId );
                break;

            case 'ft':
                // if this isn't the most popular product but does have an echoNest DPP
                $this->debugMessage( "Product is  not the most popular and but has echoNest data provider product" );
                foreach( $dataProviderProductsArray as $key => $dataProviderProduct )
                {
                    if( $dataProviderProduct['data_provider_id'] != $this->_dataProviderIdsArray['discogs'] )
                    {
                        // unset all DPPs apart from discogs
                        unset( $dataProviderProductsArray[$key] );
                    }
                }
                // mark this product archived
                $this->markProductArchivedByProductId( $assemblerProductId );

                // make sure the most popular product isn't marked archived and queue it to the assembler
                $this->markProductNotArchivedByProductId( $commonNamedProductIds[0] );
                $this->assembleProductByProductId( $commonNamedProductIds[0], true );
                break;
            case 'ff':
                // if this isn't the most popular product and it doesn't have an echoNest DPP mark it as archived
                $this->debugMessage( "Product is  not the most popular and does not have echoNest data provider product" );
                $this->markProductArchivedByProductId( $assemblerProductId );
                break;
        }

        $this->debugMessage( 'Found saved data for music product' );

        return array(
            'product'               => $productsArray,
            'product_attribute'     => $productAttributesArray,
            'data_provider_product' => $dataProviderProductsArray,
        );
    }

    /**
     * Function returns a list of product ids of artists with a given name ordered from most to least popular
     *
     * @param string $artistName
     *
     * @return array|CDbDataReader
     */
    private function listArtistsByNameOrderByPopularity( $artistName )
    {
        $artistSQL = "SELECT p.product_id, p.product_average_rating*p.product_num_ratings AS popularity FROM product p JOIN category_product cp ON p.product_id=cp.product_id
        JOIN category c ON cp.category_id=c.category_id WHERE p.product_name= :artist_name AND c.root_category_id= :music_category_id AND is_subproduct = 0
        GROUP BY p.product_id ORDER BY popularity DESC, CHAR_LENGTH(p.product_description) DESC, p.product_id;";
        $this->debugMessage( $artistSQL, 2 );
        $artistIdsArray = $this->_db->createCommand( $artistSQL )
                                    ->queryColumn(
                                        array(
                                            ':artist_name'       => $artistName,
                                            ':music_category_id' => self::musicRootId()
                                        )
                                    );
        $this->debugMessage( "Found " . count( $artistIdsArray ) . " artists with the name : " . $artistName );
        $this->debugMessage( json_encode( $artistIdsArray ), 3 );

        return $artistIdsArray;
    }

    /**
     * marks a product as not archived
     *
     * @param $productId
     */
    protected function markProductNotArchivedByProductId( $productId )
    {
        /**
         * @var Product $product
         */
        //get the product
        $product = Product::model()
                          ->findByPk( $productId );

        if( $product && $product->archived == 1 )
        {
            //mark it not archived
            $product->setAttribute( 'archived', 0 );
            $product->save();

            // queue to clear the cache for the product and update the search index
            $this->_messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $productId, true );
            $this->debugMessage( "Marked product not archived: $productId" );
        }
    }

    /**
     * marks a product as archived
     *
     * @param $productId
     */
    protected function markProductArchivedByProductId( $productId )
    {
        /**
         * get the product
         * @var $product Product
         */
        $product = Product::model()
                          ->findByPk( $productId );

        if( $product && $product->archived == 0 )
        {
            // mark it archived and not to display
            $product->setAttribute( 'archived', 1 );
            $product->setAttribute( 'display', 0 );
            $product->save();

            // queue to clear the cache for the product and update the search index
            $this->_messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $productId, true );
            $this->debugMessage( "Marked product archived: $productId" );
        }
    }

    /**
     * This function check if data provider products taken from a less popular product with the same name look like they're about this product.
     *
     * @param array $productInformation
     * @param int   $productId
     *
     * @return array
     */
    public function musicArtistInformationValidation( array $productInformation, $productId )
    {
        $this->debugMessage( "validating found data as music artist" );
        $echoNestCheck = false;

        $discogsGroundTruth = array();

        $data = array_merge( $productInformation['product'], $productInformation['product_attribute'], $productInformation['data_provider_product'] );
        foreach( $data as $object )
        {
            if( !$echoNestCheck && $object['product_id'] != $productId )
            {
                // if we have DPPs from another product (as this one is more popular) set to check that they're about the right product
                $echoNestCheck = true;
                if( !empty( $discogsGroundTruth ) )
                {
                    break;
                }
            }
            elseif( !empty( $object['data_provider_id'] ) && $object['data_provider_id'] == $this->_dataProviderIdsArray['discogs']
                    && preg_match(
                        "/^a/",
                        $object['data_provider_product_id']
                    )
            )
            {
                // we should always have a discogs product, take this opportunity to isolate it (so we don't need another foreach loop later),
                $discogsGroundTruth = $object;
                if( $echoNestCheck )
                {
                    break;
                }
            }
        }

        if( $echoNestCheck && isset( $discogsGroundTruth['product_attributes'] ) )
        {
            $this->debugMessage( "getting strings to check for" );
            $checkStringsArray = array();
            $discogsGroundTruthAttributes = json_decode( $discogsGroundTruth['product_attributes'], true );

            // so far only members (if this is blank then though)
            if( isset( $discogsGroundTruthAttributes['members'] ) && !empty( $discogsGroundTruthAttributes['members'] ) )
            {
                if( !is_array( $discogsGroundTruthAttributes['members'] ) )
                {
                    $discogsGroundTruthAttributes['members'] = array($discogsGroundTruthAttributes['members']);
                }

                foreach( $discogsGroundTruthAttributes['members'] as $member )
                {
                    if( is_array( $member ) && isset( $member['name'] ) )
                    {
                        $checkStringsArray[] = $member['name'];
                    }
                    elseif( is_string( $member ) )
                    {
                        $checkStringsArray[] = $member;
                    }
                }
            }
            if( !empty( $checkStringsArray ) )
            {
                // if we found something to check for  then check for it, otherwise just assume it's the right data (it should be)
                $this->debugMessage( "checking for : \"" . implode( '","', $checkStringsArray ) . '"', 2 );
                $echoNestCheck = false;
                foreach( $productInformation['data_provider_product'] as $dataProviderProduct )
                {
                    if( $dataProviderProduct['product_id'] != $productId
                        && preg_match(
                            "/(?:" . implode( ')|(?:', $checkStringsArray ) . ")/i",
                            $dataProviderProduct['product_description'] . $dataProviderProduct['product_attributes']
                        )
                    )
                    {
                        // search in a string made from the description and the attributes
                        $this->debugMessage( "found in : " . $dataProviderProduct['data_provider_product_id'], 2 );
                        $this->debugMessage( json_encode( $dataProviderProduct ), 3 );
                        $echoNestCheck = true;
                        break;
                    }
                }
            }
        }

        // as we didn't look for any products apart from the main one both products and product attributes should be valid
        $validProductInformation = array(
            'product'               => $productInformation['product'],
            'product_attribute'     => $productInformation['product_attribute'],
            'data_provider_product' => array(),
        );

        foreach( $productInformation['data_provider_product'] as $dataProviderProduct )
        {
            // if it's a DPP about the core product or if we found the strings we were looking for or if we didn't find any strings to look for then the DPP should be valid
            if( $dataProviderProduct['product_id'] == $productId or $echoNestCheck )
            {
                switch( $dataProviderProduct['data_provider_id'] )
                {
                    case $this->_dataProviderIdsArray['spotify']:
                    case $this->_dataProviderIdsArray['googleBooks']:
                    case $this->_dataProviderIdsArray['sevenDigital']:
                    case $this->_dataProviderIdsArray['fanArt']:
                    case $this->_dataProviderIdsArray['productDuplication']:
                        $this->debugMessage(
                            "invalid data provider product : " . $this->constructDataProviderProductIdentifier(
                                $dataProviderProduct['data_provider_id'],
                                $dataProviderProduct['data_provider_product_id'],
                                $dataProviderProduct['product_id']
                            ),
                            2
                        );
                        break;
                    default:
                        $this->debugMessage(
                            "valid data provider product : " . $this->constructDataProviderProductIdentifier(
                                $dataProviderProduct['data_provider_id'],
                                $dataProviderProduct['data_provider_product_id'],
                                $dataProviderProduct['product_id']
                            ),
                            2
                        );

                        $validProductInformation['data_provider_product'][] = $dataProviderProduct;
                        break;
                }
            }
        }
        $this->debugMessage( "validation complete" );

        return $validProductInformation;
    }

    /**
     * Function gets all information about a master product and all of its subordinate products
     *
     * @param $assemblerProductId
     *
     * @return array
     */
    public function getMasterProductInformation( $assemblerProductId )
    {
        // get data directly about this product from the database
        $productsArray = array($this->getProductData( $assemblerProductId ));
        if( empty( $productsArray ) )
        {
            $this->debugMessage( "No product found for product id : $assemblerProductId" . PHP_EOL );

            return null;
        }
        $productAttributesArray = $this->getProductAttributesArray( $assemblerProductId );
        $dataProviderProductsArray = $this->getDataProviderProductsArray( $assemblerProductId );

        if( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) )
        {
            // if the product has more than one PDDPP or its one PDDPP isn't a Master
            if( count( $dataProviderProductsArray ) != 1 or $dataProviderProductsArray[0]['data_provider_id'] != $this->_dataProviderIdsArray['productDuplication'] or $dataProviderProductsArray[0]['product_name'] != ProductDuplicationDataProvider::MASTER_PRODUCT_NAME )
            {
                $this->debugMessage( "Product has incorrect data provider products to be a master" );

                // if it has only one then it shouldn't have been directed here, throw an error
                if( count( $dataProviderProductsArray ) == 1 )
                {
                    // something has gone wrong
                    $this->debugMessage( "Product has incorrect data provider products to be a master" . PHP_EOL );

                    return null;
                }
                else
                {
                    // check there is a master PDDPP
                    $hasMasterPDDPP = false;
                    foreach( $dataProviderProductsArray as $dataProviderProduct )
                    {
                        if( $dataProviderProduct['data_provider_id'] == $this->_dataProviderIdsArray['productDuplication'] and $dataProviderProduct['product_name'] == ProductDuplicationDataProvider::MASTER_PRODUCT_NAME )
                        {
                            // keep only the master PDDPP, discard all other DataProviderProducts
                            $hasMasterPDDPP = true;
                            $dataProviderProductsArray = array($dataProviderProduct);
                            break;
                        }
                    }
                    // if there wasn't a master PDDPP
                    if( !$hasMasterPDDPP )
                    {
                        // something has gone wrong
                        $this->debugMessage( "Product has incorrect data provider products to be a master" . PHP_EOL );

                        return null;
                    }
                }
            }

            $relationshipsArray = json_decode( $dataProviderProductsArray[0]['product_attributes'], true );
            if( isset( $relationshipsArray[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
            {
                // get the master's subordinate product's
                $subordinateProductIdsArray = $relationshipsArray[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];
            }
            if( empty( $subordinateProductIdsArray ) )
            {
                $this->debugMessage( "Product has no, subordinate products, deleting duplication data provider product" );

                $productDuplicationDataProviderProduct = DataProviderProduct::model()
                                                                            ->findByAttributes( array(
                                                                                'product_id'       => $assemblerProductId,
                                                                                'data_provider_id' => $this->_dataProviderIdsArray['productDuplication']
                                                                            ) );
                $productDuplicationDataProviderProduct->delete();

                return null;
            }

            // for each subordinate
            foreach( $subordinateProductIdsArray as $subordinateProductId )
            {
                if( $subordinateProductData = $this->getProductData( $subordinateProductId ) )
                {
                    // get its details
                    $productsArray = array_merge( $productsArray, array($subordinateProductData) );
                    $productAttributesArray = array_merge( $productAttributesArray, $this->getProductAttributesArray( $subordinateProductId ) );
                    $dataProviderProductsArray = array_merge( $dataProviderProductsArray, $this->getDataProviderProductsArray( $subordinateProductId ) );
                }
                else
                {
                    $this->_productDuplicationDataProvider->repairDuplicateRelationships( $assemblerProductId );
                }
            }
        }
        elseif( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationService' ) )
        {
            /**
             * @var $this ->_productDuplicationDataProvider ProductDuplicationService
             */
            $productRelations = $this->_productDuplicationDataProvider->getProductRelations( $assemblerProductId );

            //check the product is master and not conflicted
            if( isset( $productRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
            {

                $subordinateProductIdsArray = $productRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];
                // for each subordinate
                foreach( $subordinateProductIdsArray as $subordinateProductId )
                {
                    if( $subordinateProductData = $this->getProductData( $subordinateProductId ) )
                    {
                        // get its details
                        $productsArray = array_merge( $productsArray, array($subordinateProductData) );
                        $productAttributesArray = array_merge( $productAttributesArray, $this->getProductAttributesArray( $subordinateProductId ) );
                        $dataProviderProductsArray = array_merge( $dataProviderProductsArray, $this->getDataProviderProductsArray( $subordinateProductId ) );
                    }
                    else
                    {
                        $this->_productDuplicationDataProvider->repairDuplicateRelationships( $assemblerProductId );
                    }
                }

            }
        }

        return array(
            'product'               => $productsArray,
            'product_attribute'     => $productAttributesArray,
            'data_provider_product' => $dataProviderProductsArray,
        );
    }

    /**
     * Function validates information for a product that is a master.
     *
     * @param array $productInformation
     * @param int   $productId
     *
     * @return array
     */
    public function masterProductInformationValidation( array $productInformation, $productId )
    {

        if( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) )
        {
        // construct an array linking each product id to a type of product
        $validProductIdsArray = array($productId => ProductDuplicationDataProvider::MASTER_PRODUCT_NAME);
        foreach( $productInformation['data_provider_product'] as $dataProviderProductKey => $dataProviderProduct )
        {
            if( $dataProviderProduct['data_provider_id'] == $this->_dataProviderIdsArray['productDuplication']
                && $dataProviderProduct['product_name'] == ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME
                && json_decode( $dataProviderProduct['product_attributes'], true )[ProductDuplicationDataProvider::SUBORDINATE_MASTER_PRODUCT_ID_KEY] == $productId
            )
            {
                $this->debugMessage( "Valid subordinate : " . $dataProviderProduct['product_id'] );
                $validProductIdsArray[$dataProviderProduct['product_id']] = ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME;
            }
        }
        $this->debugMessage( "validating product information as a master product" );
        $validProductInformation = array(
            'product'               => array(),
            'product_attribute'     => array(),
            'data_provider_product' => array(),
        );
        foreach( $productInformation['product'] as $productKey => $product )
        {
            switch( empty( $validProductIdsArray[$product['product_id']] ) ?
                "Invalid" :
                $validProductIdsArray[$product['product_id']] )
            {
                case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                case ProductDuplicationDataProvider::MASTER_PRODUCT_NAME:
                    $this->debugMessage(
                        "valid product : " . $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] ),
                        2
                    );
                    $validProductInformation['product'][$productKey] = $product;
                    break;
                default:
                    $this->debugMessage(
                        "invalid product : " . $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] ),
                        2
                    );
                    break;
            }
        }
        foreach( $productInformation['product_attribute'] as $productAttributeKey => $productAttribute )
        {
            switch( empty( $validProductIdsArray[$productAttribute['product_id']] ) ?
                "Invalid" :
                $validProductIdsArray[$productAttribute['product_id']] )
            {
                case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                case ProductDuplicationDataProvider::MASTER_PRODUCT_NAME:
                    switch( $productAttribute['product_attribute_type_name'] )
                    {
                        default:
                            $this->debugMessage(
                                "valid product attribute : " . $this->constructDataProviderProductIdentifier(
                                    0,
                                    'attribute_id:' . $productAttribute['id'],
                                    $productAttribute['product_id']
                                ),
                                2
                            );
                            $validProductInformation['product_attribute'][$productAttributeKey] = $productAttribute;
                            break 2;
                    }
                default:
                    $this->debugMessage(
                        "invalid product attribute : " . $this->constructDataProviderProductIdentifier(
                            0,
                            'attribute_id:' . $productAttribute['id'],
                            $productAttribute['product_id']
                        ),
                        2
                    );
                    break;
            }
        }
        foreach( $productInformation['data_provider_product'] as $dataProviderProductKey => $dataProviderProduct )
        {
            switch( empty( $validProductIdsArray[$dataProviderProduct['product_id']] ) ?
                "Invalid" :
                $validProductIdsArray[$dataProviderProduct['product_id']] )
            {
                case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                    switch( $dataProviderProduct['data_provider_id'] )
                    {
                        case $this->_dataProviderIdsArray['spotify']:
                        case $this->_dataProviderIdsArray['googleBooks']:
                        case $this->_dataProviderIdsArray['sevenDigital']:
                        case $this->_dataProviderIdsArray['fanArt']:
                        case $this->_dataProviderIdsArray['productDuplication']:
                            $this->debugMessage(
                                "invalid data provider product : " . $this->constructDataProviderProductIdentifier(
                                    $dataProviderProduct['data_provider_id'],
                                    $dataProviderProduct['data_provider_product_id'],
                                    $dataProviderProduct['product_id']
                                ),
                                2
                            );
                            break 2;
                        default:
                            $this->debugMessage(
                                "valid data provider product : " . $this->constructDataProviderProductIdentifier(
                                    $dataProviderProduct['data_provider_id'],
                                    $dataProviderProduct['data_provider_product_id'],
                                    $dataProviderProduct['product_id']
                                ),
                                2
                            );
                            $validProductInformation['data_provider_product'][$dataProviderProductKey] = $dataProviderProduct;
                            break 2;
                    }
                default:
                    $this->debugMessage(
                        "invalid data provider product : " . $this->constructDataProviderProductIdentifier(
                            $dataProviderProduct['data_provider_id'],
                            $dataProviderProduct['data_provider_product_id'],
                            $dataProviderProduct['product_id']
                        ),
                        2
                    );
                    break;
            }
        }
        }
        elseif( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationService' ) )
        {
            // construct an array linking each product id to a type of product
            $validProductIdsArray = array($productId => ProductDuplicationDataProvider::MASTER_PRODUCT_NAME);
            $validRelations = $this->_productDuplicationDataProvider->getProductRelations( $productId );

            if( isset( $validRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
            {
                foreach( $validRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] as $productId )
                {
                    $this->debugMessage( "Valid subordinate : " . $dataProviderProduct['product_id'] );
                    $validProductIdsArray[$productId] = ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME;
                }
            }
            $this->debugMessage( "validating product information as a master product" );
            $validProductInformation = array(
                'product'               => array(),
                'product_attribute'     => array(),
                'data_provider_product' => array(),
            );

            //validate product info is in the master product id or sub
            foreach( $productInformation['product'] as $productKey => $product )
            {
                switch( empty( $validProductIdsArray[$product['product_id']] ) ? "Invalid" : $validProductIdsArray[$product['product_id']] )
                {
                    case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                    case ProductDuplicationDataProvider::MASTER_PRODUCT_NAME:
                        $this->debugMessage( "valid product : " . $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] ), 2 );
                        $validProductInformation['product'][$productKey] = $product;
                        break;
                    default:
                        $this->debugMessage( "invalid product : " . $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] ), 2 );
                        break;
                }
            }

            //validate product attribute  is in the master product id or sub
            foreach( $productInformation['product_attribute'] as $productAttributeKey => $productAttribute )
            {

                switch( empty( $validProductIdsArray[$productAttribute['product_id']] ) ? "Invalid" : $validProductIdsArray[$productAttribute['product_id']] )
                {
                    case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                    case ProductDuplicationDataProvider::MASTER_PRODUCT_NAME:
                        switch( $productAttribute['product_attribute_type_name'] )
                        {
                            default:
                                $this->debugMessage( "valid product attribute : " . $this->constructDataProviderProductIdentifier( 0, 'attribute_id:' . $productAttribute['id'], $productAttribute['product_id'] ), 2 );
                                $validProductInformation['product_attribute'][$productAttributeKey] = $productAttribute;
                                break 2;
                        }
                    default:
                        $this->debugMessage( "invalid product attribute : " . $this->constructDataProviderProductIdentifier( 0, 'attribute_id:' . $productAttribute['id'], $productAttribute['product_id'] ), 2 );
                        break;
                }
            }
            //validate product attribute  is in the master product id or sub and not from spotify, googleBooks, sevenDigital, fanArt or ProductDuplication
            foreach( $productInformation['data_provider_product'] as $dataProviderProductKey => $dataProviderProduct )
            {
                switch( empty( $validProductIdsArray[$dataProviderProduct['product_id']] ) ? "Invalid" : $validProductIdsArray[$dataProviderProduct['product_id']] )
                {
                    case ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME:
                        switch( $dataProviderProduct['data_provider_id'] )
                        {
                            case $this->_dataProviderIdsArray['spotify']:
                            case $this->_dataProviderIdsArray['googleBooks']:
                            case $this->_dataProviderIdsArray['sevenDigital']:
                            case $this->_dataProviderIdsArray['fanArt']:
                            case $this->_dataProviderIdsArray['productDuplication']:
                                $this->debugMessage( "invalid data provider product : " . $this->constructDataProviderProductIdentifier( $dataProviderProduct['data_provider_id'], $dataProviderProduct['data_provider_product_id'], $dataProviderProduct['product_id'] ), 2 );
                                break 2;
                            default:
                                $this->debugMessage( "valid data provider product : " . $this->constructDataProviderProductIdentifier( $dataProviderProduct['data_provider_id'], $dataProviderProduct['data_provider_product_id'], $dataProviderProduct['product_id'] ), 2 );
                                $validProductInformation['data_provider_product'][$dataProviderProductKey] = $dataProviderProduct;
                                break 2;
                        }
                    default:
                        $this->debugMessage( "invalid data provider product : " . $this->constructDataProviderProductIdentifier( $dataProviderProduct['data_provider_id'], $dataProviderProduct['data_provider_product_id'], $dataProviderProduct['product_id'] ), 2 );
                        break;
                }
            }
        }

        //return validated info

        return $validProductInformation;
    }

    /**
     * groups all data into a single array with the source of each piece of data given by an identifier that specifies data provider id, data provider product id and product id
     *
     * @param array $productInformation
     * @param array $rejectedAttributes
     *
     * @return array
     */
    public function processRawProductData( array $productInformation, array &$rejectedAttributes = null )
    {
        $this->debugMessage( "Collecting product data" );
        $rawProduct = array();

        // Extract the necessary data from the data provider products where possible
        foreach( $productInformation['data_provider_product'] as $dataProviderProduct )
        {
            $this->collectDataProviderProductData( $dataProviderProduct, $rawProduct, $rejectedAttributes );
        }

        // Extract the necessary data from the current database products where possible
        foreach( $productInformation['product'] as $product )
        {
            $this->collectDatabaseProductData( $product, $rawProduct );
        }

        // Extract the necessary data from the current database product attributes where possible
        foreach( $productInformation['product_attribute'] as $productAttribute )
        {
            $attributeIdentifier = $this->constructDataProviderProductIdentifier( 0, 'attribute_id:' . $productAttribute['id'], $productAttribute['product_id'] );
            $this->debugMessage( "Collecting data form database product attribute : $attributeIdentifier", 2 );
            $rawProduct['product_attributes'][$attributeIdentifier] = $this->checkProductAttributeValues(
                array($productAttribute['product_attribute_type_name'] => $productAttribute['product_attribute_value'])
            );

            $this->debugMessage( json_encode( $productAttribute['product_attribute_value'] ), 3 );

            if( empty( $rawProduct['product_attributes'][$attributeIdentifier][$productAttribute['product_attribute_type_name']] ) )
            {
                unset( $rawProduct['product_attributes'][$attributeIdentifier][$productAttribute['product_attribute_type_name']] );
            }
        }

        return $rawProduct;
    }

    /**
     * Function that extracts permitted data from a data provider product into $rawProductData. Also collects rejected data into $rejectedAttributesArray if passed.
     *
     * @param array $dataProviderProduct
     * @param array $rawProductData
     * @param array $rejectedAttributesArray
     *
     * @return array
     */
    public function collectDataProviderProductData( array $dataProviderProduct, array &$rawProductData, array &$rejectedAttributesArray = null )
    {
        // generate the unique identifier
        $dataProviderProductIdentifier = $this->constructDataProviderProductIdentifier(
            $dataProviderProduct['data_provider_id'],
            $dataProviderProduct['data_provider_product_id'],
            $dataProviderProduct['product_id']
        );

        $this->debugMessage( "Collecting data from data provider product : $dataProviderProductIdentifier", 2 );
        $this->debugMessage( print_r( $dataProviderProduct, true ), 3 );

        $rejectedAttributesArray[$dataProviderProductIdentifier] = array();

        if( !empty( $dataProviderProduct['product_name'] ) )
        {
            // get name if possible
            $rawProductData['product_name'][$dataProviderProductIdentifier] = $dataProviderProduct['product_name'];
            $this->debugMessage( "name: " . $dataProviderProduct['product_name'], 3 );
        }

        if( !empty( $dataProviderProduct['product_description'] ) && $dataProviderProduct['product_description'] != self::$_NO_DESCRIPTION_YET )
        {
            // get description if set
            $rawProductData['product_description'][$dataProviderProductIdentifier] = $dataProviderProduct['product_description'];
            $this->debugMessage( "description: " . $dataProviderProduct['product_description'], 3 );
        }
        if( !empty( $dataProviderProduct['product_attributes'] ) )
        {
            // get valid attributes and add invalid attributes to rejectedAttributesArray
            $rawProductData['product_attributes'][$dataProviderProductIdentifier] = $this->processDataProviderProductAttributesArray( json_decode( $dataProviderProduct['product_attributes'], true ) ?: json_decode( $dataProviderProduct['product_attributes'] . '"}', true ), $rejectedAttributesArray[$dataProviderProductIdentifier] );
            $this->debugMessage( "attributes: " . json_encode( $rawProductData['product_attributes'][$dataProviderProductIdentifier] ), 3 );
            $this->debugMessage( "rejected attributes: " . json_encode( $rejectedAttributesArray[$dataProviderProductIdentifier] ), 3 );
        }

        if( empty( $rejectedAttributesArray[$dataProviderProductIdentifier] ) )
        {
            // if there were no rejected attributes for this DPP unset that entry in the array
            unset( $rejectedAttributesArray[$dataProviderProductIdentifier] );
        }
    }

    /**
     * Function validates that the attributes passed to it have permitted attribute names. Invalid attributes are added to $rejectedDataProviderProductAttributesArray.
     *
     * @param array $unprocessedProductAttributesArray
     * @param array $rejectedDataProviderProductAttributesArray
     *
     * @return array
     */
    public function processDataProviderProductAttributesArray( array $unprocessedProductAttributesArray, array &$rejectedDataProviderProductAttributesArray = null )
    {
        $productAttributesArray = array();

        if( !empty( $unprocessedProductAttributesArray ) )
        {
            foreach( $unprocessedProductAttributesArray as $attributeName => $attributeValue )
            {
                // change attribute names to the correct version/format
                $productAttributesArray = array_merge( $productAttributesArray, $this->processDataProviderProductAttribute( $attributeName, $attributeValue ) );
            }
        }

        // unset invalid attributes, adding them to $rejectedDataProviderProductAttributesArray if supplied
        $allowedProductAttributesArray = $this->checkProductAttributeNames( $productAttributesArray, $rejectedDataProviderProductAttributesArray );

        // check that the product attributes are as we expect them
        $approvedProductAttributeArray = $this->checkProductAttributeValues( $allowedProductAttributesArray );

        // return the processed data
        return $approvedProductAttributeArray;
    }

    /**
     * function changes the attribute name to the correct version if possible and the correct format if not.
     *
     * @param string $attributeName
     * @param string $attributeValue
     *
     * @return mixed
     */
    protected function processDataProviderProductAttribute( $attributeName, $attributeValue )
    {
        // if we have a direct mapping (i.e. "NumberOfPages" from the data provider maps to "pageCount" in our DB then process is
        if( isset( $this->_attributeMappings[$attributeName] ) )
        {
            $productAttribute[$this->_attributeMappings[$attributeName]] = $attributeValue;
        }
        else
        {
            // otherwise camelCase the name and set the value without modifying, since
            // these only get saved if they're referenced in the product_attribute_type table
            if( !isset( $productAttribute[lcfirst( ucwords( $attributeName ) )] ) )
            {
                $productAttribute[lcfirst( ucwords( $attributeName ) )] = $attributeValue;
            }
        }

        return $productAttribute;
    }

    /**
     * Function filters out invalid attributes, adding them to $rejectedDataProviderProductAttributesArray
     *
     * @param array $productAttributesArray
     * @param array $rejectedDataProviderProductAttributesArray
     *
     * @return array
     */
    protected function checkProductAttributeNames( array $productAttributesArray, array &$rejectedDataProviderProductAttributesArray )
    {
        // loop through the attributes
        foreach( $productAttributesArray as $attributeName => $attributeValue )
        {
            // get the attributeTypeId if it exists
            $attributeTypeId = ProductAttributeType::model()
                                                   ->getIdByName( $attributeName );

            if( empty( $attributeTypeId ) )
            {
                $rejectedDataProviderProductAttributesArray[$attributeName] = $attributeValue;
                unset( $productAttributesArray[$attributeName] );
            }

        }

        return $productAttributesArray;
    }

    /**
     * this function checks the value of each product attribute (in most cases it passes it with no changes)
     *
     * @param array $productAttributesArray
     *
     * @return array
     */
    protected function checkProductAttributeValues( array $productAttributesArray )
    {
        $approvedAttributes = array();

        // for each attribute
        foreach( $productAttributesArray as $key => $value )
        {
            // skip empty values
            if( !empty( $value ) )
            {
                // take different actions depending on what the attribute type name is
                switch( $key )
                {
                    // if it's a related users array
                    case 'related_user':
                        // if it's an array where the values and keys are the same
                        if( is_array( $value ) && array_keys( $value ) == ( $valueValues = array_values( $value ) ) )
                        {
                            // keep only the values (make it an indexed array)
                            $approvedAttributes[$key] = $valueValues;
                        }
                        else
                        {
                            // no changes
                            $approvedAttributes[$key] = $value;
                        }
                        break;

                    // if it's an about attribute
                    case 'about':
                        // if it's a string
                        if( is_string( $value ) )
                        {
                            // if it json_decodes well
                            if( $decodedValue = json_decode( $value, true ) )
                            {
                                // use the decoded value as is
                                $approvedAttributes[$key] = $decodedValue;
                            }
                            // if not
                            else
                            {
                                // treat the string as a description
                                $approvedAttributes[$key]['description'] = $value;
                            }
                        }
                        else
                        {
                            // use it as is
                            $approvedAttributes[$key] = $value;
                        }
                        break;

                    // if it's an info attribute
                    case 'info':
                        // if the value's a string
                        if( is_string( $value ) )
                        {
                            //json_decode it
                            $info = json_decode( $value, true );
                        }
                        // if not
                        else
                        {
                            // use it as is
                            $info = $value;
                        }
                        $desiredInfo = array();

                        // if it's an indexed array
                        if( array_keys( $info ) == range( 0, ( count( $info ) - 1 ) ) )
                        {
                            // for each piece of info
                            foreach( $info as $infoBit )
                            {
                                // if it has a title and non-empty value
                                if( isset( $infoBit['title'] ) && !empty( $infoBit['value'] ) )
                                {
                                    // if the value's an array already
                                    if( is_array( $infoBit['value'] ) )
                                    {
                                        // titleCase the title and use the value as is
                                        $desiredInfo[] = array(
                                            'title' => Utilities::strToTitle( $infoBit['title'] ),
                                            'value' => $infoBit['value']
                                        );
                                    }
                                    else
                                    {
                                        //titleCase the title and turn the value into a single element array
                                        $desiredInfo[] = array(
                                            'title' => Utilities::strToTitle( $infoBit['title'] ),
                                            'value' => array($infoBit['value'])
                                        );
                                    }
                                }
                            }
                        }
                        // if it's an associative array
                        else
                        {
                            // for each piece of info
                            foreach( $info as $infoTitle => $infoValue )
                            {
                                // if it's not empty
                                if( !empty( $infoValue ) )
                                {
                                    // if it's an array
                                    if( is_array( $infoValue ) )
                                    {
                                        // Title case the key and use the vlaue as is
                                        $desiredInfo[] = array(
                                            'title' => Utilities::strToTitle( $infoTitle ),
                                            'value' => $infoValue
                                        );
                                    }
                                    // if it's not an arry
                                    else
                                    {
                                        // title case the key and add the value to a one element array
                                        $desiredInfo[] = array(
                                            'title' => Utilities::strToTitle( $infoTitle ),
                                            'value' => array($infoValue)
                                        );
                                    }
                                }
                            }
                        }

                        // if we got some info
                        if( !empty( $desiredInfo ) )
                        {
                            // add it to the approved attributes
                            $approvedAttributes[$key] = $desiredInfo;
                        }
                        break;

                    default:
                        // if nothing specific has been set to do then just pass the attribute as is
                        $approvedAttributes[$key] = $value;
                }
            }
        }

        return $approvedAttributes;
    }

    /**
     * function extracts data from a database product and adds it to $rawProduct
     *
     * @param array $product
     * @param array $rawProduct
     */
    public function collectDatabaseProductData( array $product, array &$rawProduct )
    {
        $productIdentifier = $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $product['product_id'], $product['product_id'] );
        $this->debugMessage( "Collecting data from database product : $productIdentifier", 2 );
        $this->debugMessage( print_r( $product, true ), 3 );
        foreach( $product as $key => $value )
        {
            if( !empty( $value ) )
            {
                switch( $key )
                {
                    // for each attribute in the case list add it to $rawProduct
                    case 'is_subproduct':
                    case 'release_date':
                    case 'product_name':
                        $rawProduct[$key][$productIdentifier] = $value;
                        $this->debugMessage( "$key : $value", 3 );
                        break;
                    case 'product_description':
                        if( $value != self::$_NO_DESCRIPTION_YET )
                        {
                            $rawProduct[$key][$productIdentifier] = $value;
                            $this->debugMessage( "$key : $value", 3 );
                        }
                }
            }
        }
    }

    /**
     * function changes the order of the array keys of the attributes so that it's optimal for the actual assembling step.
     *
     * @param array $rawProduct
     *
     * @return array
     */
    public function formatRawProductData( array $rawProduct )
    {
        $this->debugMessage( "Formatting product data" );
        $formattedRawProduct = array();

        // name and description stay as is
        if( !empty( $rawProduct['product_name'] ) )
        {
            $formattedRawProduct['product_name'] = $rawProduct['product_name'];
        }
        else
        {
            $formattedRawProduct['product_name'] = array();
        }
        if( !empty( $rawProduct['product_description'] ) )
        {
            $formattedRawProduct['product_description'] = $rawProduct['product_description'];
        }
        if( !empty( $rawProduct['release_date'] ) )
        {
            $formattedRawProduct['release_date'] = $rawProduct['release_date'];
        }
        if( !empty( $rawProduct['is_subproduct'] ) )
        {
            $formattedRawProduct['is_subproduct'] = $rawProduct['is_subproduct'];
        }

        if( !empty( $rawProduct['product_attributes'] ) )
        {
            // each attribute needs to be formatted so that the alternative DPP identifiers are at the depth in the array that you want to choose between them.
            foreach( $rawProduct['product_attributes'] as $dataProviderProductIdentifier => $dataProviderProductAttributeArray )
            {
                foreach( $dataProviderProductAttributeArray as $productAttributeName => $dataProviderProductAttribute )
                {
                    /*
                     * Had to turn of E_NOTICE for this line to suppress integer overflow warnings
                     */
                    $reportMode = error_reporting(); // save current error reporting
                    error_reporting( $reportMode & ~E_NOTICE ); // turn off E_NOTICE level errors
                    // try to json_decode the attribute, don't if you can't
                    //if is array just use the attribute, if not attempt to decode, if not possible  add } and attempt to decode, if not possible use the value as is
                    $productAttribute = ( is_string( $dataProviderProductAttribute ) ) ?
                        ( json_decode( $dataProviderProductAttribute, true, 512, JSON_BIGINT_AS_STRING ) ?:
                            ( json_decode( $dataProviderProductAttribute . '"}', true, 512, JSON_BIGINT_AS_STRING ) ?:
                                ( $dataProviderProductAttribute ) ) ) :
                        $dataProviderProductAttribute;
                    error_reporting( $reportMode );// go back to old error reporting
                    if( is_array( $productAttribute ) && Utilities::is_assoc( array($productAttribute) ) )
                    {
                        foreach( $productAttribute as $subAttributeName => $subAttributeValue )
                        {
                            // if an attribute is an associative array compare each sub attribute
                            $formattedRawProduct['product_attributes'][$productAttributeName][$subAttributeName][$dataProviderProductIdentifier] = $subAttributeValue;
                        }
                    }
                    else
                    {
                        // if it isn't an associative array comare the attribute as a whole
                        $formattedRawProduct['product_attributes'][$productAttributeName][$dataProviderProductIdentifier] = $productAttribute;
                    }
                }
            }
        }
        $this->debugMessage( json_encode( $formattedRawProduct ), 3 );

        return $formattedRawProduct;
    }

    /**
     * This function directs the decisions about what to put into the final product
     *
     * @param array $rawProduct
     * @param int   $productId
     * @param int   $rootCategoryId
     *
     * @return array
     */
    public function assembleProduct( array $rawProduct, $productId, $rootCategoryId/*, array $rejectedAttributes = null */ )
    {
        $assembledProductIdentifier = $this->constructDataProviderProductIdentifier( 0, 'product_id:' . $productId, $productId );

        $this->debugMessage( "\nAssembling Product" );
        $assembledProduct = array();

        // decide the name
        $assembledProduct['product_name'] = $this->decideProductName( $rawProduct, $productId, $rootCategoryId );
        $this->debugMessage( "Chosen name : " . $assembledProduct['product_name'] );

        // decide the description
        $assembledProduct['product_description'] = $this->decideProductDescription( $rawProduct, $productId, $rootCategoryId );
        $this->debugMessage( "Chosen description length : " . strlen( $assembledProduct['product_description'] ) );

        $assembledProduct['release_date'] = $this->decideReleaseDate( $rawProduct );
        $this->debugMessage( "Chosen release date : " . $assembledProduct['release_date'] );

        $assembledProduct['product_root_category_id'] = $rootCategoryId;

        if( !empty( $rawProduct['is_subproduct'][$assembledProductIdentifier] ) )
        {
            $assembledProduct['is_subproduct'] = 1;
        }
        else
        {
            $assembledProduct['is_subproduct'] = 0;
        }

        if( !empty( $rawProduct['product_attributes'] ) )
        {
            // decide the attributes
            $assembledProduct['product_attributes'] = $this->decideProductAttributeArray( $rawProduct, $rootCategoryId );
            $this->debugMessage( "Chosen " . count( $assembledProduct['product_attributes'] ) . " product attributes" );
        }
        else
        {
            $assembledProduct['product_attributes'] = array();
            $this->debugMessage( "No product attributes found" );
        }
        $this->debugMessage( 'Finished Assembling Product' );
        $this->debugMessage( json_encode( $assembledProduct ), 3 );

        return $assembledProduct;
    }

    /**
     * this function decides what name to give a product
     *
     * @param array $processingProduct
     * @param int   $productId
     * @param int   $rootCategoryId
     *
     * @return mixed
     */
    public function decideProductName( array $processingProduct, $productId, $rootCategoryId )
    {
        $this->debugMessage( "Deciding product name" );

        // check if any DPPs have a name for the product
        $hasDataProviderProductName = false;
        foreach( $processingProduct['product_name'] as $dataProviderProductIdentifier => $dataProviderProductName )
        {
            if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] != 0 )
            {
                $hasDataProviderProductName = true;
                break;
            }
        }
        $this->debugMessage(
            "Product " . ( ( $hasDataProviderProductName ) ?
                'has' :
                'does not have' ) . " data providers for its name",
            2
        );
        switch( $rootCategoryId )
        {
            case self::musicRootId():
                if( $this->isSubproduct( $productId ) )
                {
                    return $this->defaultNameDecision( $processingProduct['product_name'], $hasDataProviderProductName );
                }
                else
                {
                    return $this->musicNameDecision( $processingProduct['product_name'], $hasDataProviderProductName );
                }
            default:
                return $this->defaultNameDecision( $processingProduct['product_name'], $hasDataProviderProductName );
        }
    }

    /**
     * This function decides the product name by picking the most popular one from the data providers. It breaks ties by picking the longest name.
     *
     * @param array $nameArray
     * @param bool  $ignoreProductNames
     *
     * @return mixed
     */
    public function defaultNameDecision( array $nameArray, $ignoreProductNames = false )
    {
        $this->debugMessage( "Using default name selector" );
        $potentialNameArray = array();
        foreach( $nameArray as $dataProviderProductIdentifier => $dataProviderProductName )
        {
            if( $ignoreProductNames && explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 0 )
            {
                // if we have a DPP name and this is a name from the product table then skip it
                continue;
            }
            $this->debugMessage( "getting name from : $dataProviderProductIdentifier", 2 );
            // strip out unpaired brackets from the end of the name
            $explodedName = explode( '(', $dataProviderProductName );
            if( ( $count = count( $explodedName ) ) > 1 && !strpos( $explodedName[( $count - 1 )], ')' ) )
            {
                unset( $explodedName[$count - 1] );
                $dataProviderProductName = implode( '(', $explodedName );
            }
            unset( $count, $explodedName );
            // also strip out ending brackets and obvious author names
            $potentialName = trim( preg_replace( $this->productNameRegexPatterns, $this->productNameRegexReplacements, $dataProviderProductName ) );
            if( isset( $potentialNameArray[$potentialName] ) )
            {
                $potentialNameArray[$potentialName] += 256;
            }
            else
            {
                // max possible name length is 128 so 256 per DPP with the name guarantees the most popular name will be picked before the longest
                $potentialNameArray[$potentialName] = 256 + strlen( $potentialName );
            }
            $this->debugMessage( "name : $potentialName", 3 );
        }
        arsort( $potentialNameArray );

        return key( $potentialNameArray );
    }

    /**
     * This function decides the product name for a music product. It's non interventionist.
     *
     * @param array $nameArray
     * @param bool  $ignoreProductNames
     *
     * @return mixed
     */
    public function musicNameDecision( array $nameArray, $ignoreProductNames = false )
    {
        $this->debugMessage( "Using music name selector" );
        foreach( $nameArray as $dataProviderProductIdentifier => $dataProviderProductName )
        {
            if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 0 )
            {
                $this->debugMessage( "Choosing database name", 2 );
                // if we have a name from the database product use that

                //clean up unclosed brackets. will run into problems with (() and (()) etc
                $explodedName = explode( '(', $dataProviderProductName );
                if( ( $count = count( $explodedName ) ) > 1 && !strpos( $explodedName[( $count - 1 )], ')' ) )
                {
                    unset( $explodedName[$count - 1] );
                    $dataProviderProductName = implode( '(', $explodedName );
                }
                unset( $count, $explodedName );
                // also strip out ending brackets and obvious author names
                $potentialName = trim( preg_replace( $this->productNameRegexPatterns, $this->productNameRegexReplacements, $dataProviderProductName ) );

                return $potentialName;
            }
        }

        foreach( $nameArray as $dataProviderProductIdentifier => $dataProviderProductName )
        {
            if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == $this->_dataProviderIdsArray['discogs'] )
            {
                $this->debugMessage( "Choosing discogs name", 2 );
                // if we have a name from discogs use that

                //clean up unclosed brackets. will run into problems with (() and (()) etc
                $explodedName = explode( '(', $dataProviderProductName );
                if( ( $count = count( $explodedName ) ) > 1 && !strpos( $explodedName[( $count - 1 )], ')' ) )
                {
                    unset( $explodedName[$count - 1] );
                    $dataProviderProductName = implode( '(', $explodedName );
                }
                unset( $count, $explodedName );
                // also strip out ending brackets and obvious author names
                $potentialName = trim( preg_replace( $this->productNameRegexPatterns, $this->productNameRegexReplacements, $dataProviderProductName ) );

                return $potentialName;
            }
        }

        // if necessary go with the default decider
        return $this->defaultNameDecision( $nameArray, $ignoreProductNames );
    }

    /**
     * This function decides the product description
     *
     * @param array $processingProduct
     * @param int   $productId
     * @param int   $rootCategoryId
     *
     * @return mixed
     */
    public function decideProductDescription( array $processingProduct, $productId, $rootCategoryId )
    {
        $this->debugMessage( "Deciding product description" );

        // look for DPPs for potential product descriptions
        $hasDataProviderProductDescription = false;
        if( isset( $processingProduct['product_description'] ) )
        {
            foreach( $processingProduct['product_description'] as $dataProviderProductIdentifier => $dataProviderProductDescription )
            {
                $dataProviderId = explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0];
                //if an adminUser user set the description use that one and bail on this process
                if( $dataProviderId == DataProvider::model()
                                                   ->getDataProviderFromName( self::ADMIN_USER_DATA_PROVIDER_NAME )
                )
                {
                    return $dataProviderProductDescription;
                }
                if( $dataProviderId != 0 )
                {
                    // look for straight product_description
                    $hasDataProviderProductDescription = true;
                    break;
                }
            }
        }
        if( !$hasDataProviderProductDescription && isset( $processingProduct['product_attributes']['wikipedia']['Introduction'] ) )
        {
            foreach( $processingProduct['product_attributes']['wikipedia']['Introduction'] as $dataProviderProductIdentifier => $dataProviderProductWiki )
            {
                if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] != 0 )
                {
                    // look for a wikipedia introduction
                    $hasDataProviderProductDescription = true;
                    break;
                }
            }
        }
        if( !$hasDataProviderProductDescription && isset( $processingProduct['product_attributes']['product_description'] ) )
        {
            foreach( $processingProduct['product_attributes']['product_description'] as $dataProviderProductIdentifier => $dataProviderProductDescriptionAttribute )
            {

                if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] != 0 )
                {
                    // look for a product description attribute
                    $hasDataProviderProductDescription = true;
                    break;
                }

            }
        }
        if( !$hasDataProviderProductDescription && isset( $processingProduct['product_attributes']['about']['description'] ) )
        {
            foreach( $processingProduct['product_attributes']['about']['description'] as $dataProviderProductIdentifier => $dataProviderProductAboutAttribute )
            {
                if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] != 0 )
                {
                    // look for an about attribute
                    $hasDataProviderProductDescription = true;

                    break;
                }
            }
        }
        $this->debugMessage(
            "Product " . ( ( $hasDataProviderProductDescription ) ?
                'has' :
                'does not have' ) . " data providers for its description",
            2
        );

        // depending on the root category id choose the method to
        switch( $rootCategoryId )
        {
            case self::musicRootId():
                if( $this->isSubproduct( $productId ) )
                {
                    return $this->musicMediaDescriptionDecision( $processingProduct, $hasDataProviderProductDescription );
                }
                elseif( $hasDataProviderProductDescription )
                {
                    return $this->defaultDescriptionDecision( $processingProduct, $rootCategoryId, $hasDataProviderProductDescription );
                }
                else
                {
                    return $this->musicArtistDiscographyDescription( $processingProduct );
                }
                break;
            case self::tvShowsRootId():
                if( $this->isSubproduct( $productId ) )
                {
                    return $this->tvSubproductDescriptionDecision( $processingProduct, $rootCategoryId, $hasDataProviderProductDescription, $productId );
                }
                else
                {
                    return $this->defaultDescriptionDecision( $processingProduct, $rootCategoryId, $hasDataProviderProductDescription );
                }

            default:
                return $this->defaultDescriptionDecision( $processingProduct, $rootCategoryId, $hasDataProviderProductDescription );
        }
    }

    /**
     * Function creates a description for music subproducts
     *
     * @param array $processingProduct
     * @param bool  $ignoreProductDescriptions
     *
     * @return mixed
     */
    private function musicMediaDescriptionDecision( array $processingProduct, $ignoreProductDescriptions )
    {
        $this->debugMessage( "Using music media description selector" );
        // get the description as normal
        $description = $this->defaultDescriptionDecision( $processingProduct, $ignoreProductDescriptions );

        // if we have a track listing among the attributes
        if( isset( $processingProduct['product_attributes']['tracks'] ) )
        {
            $this->debugMessage( "Adding track listing to description", 2 );
            $tracksArray = array();
            $nonDataProviderTracksArray = array();
            // get each 'tracks' attribute
            foreach( $processingProduct['product_attributes']['tracks'] as $sourceIdentifier => $sourceTracks )
            {
                if( explode( ProductAssemblerService::SOURCE_IDENTIFIER_DELIMITER, $sourceIdentifier )[0] )
                {
                    // if it's a dataProvider source
                    foreach( $sourceTracks as $sourceTrack )
                    {
                        if( !isset( $tracksArray[$sourceTrack['position']] ) )
                        {
                            // add it to the array
                            $tracksArray[$sourceTrack['position']] = $sourceTrack;
                        }
                    }
                }
                else
                {
                    // if not
                    foreach( $sourceTracks as $sourceTrack )
                    {
                        if( !isset( $tracksArray[$sourceTrack['position']] ) )
                        {
                            // add it to the backup array
                            $nonDataProviderTracksArray[$sourceTrack['position']] = $sourceTrack;
                        }
                    }
                }
            }

            if( empty( $tracksArray ) && !empty( $nonDataProviderTracksArray ) )
            {
                // if the array is empty but the backup isn't use the backup instead
                $tracksArray = $nonDataProviderTracksArray;
            }

            if( !empty( $tracksArray ) )
            {
                // attempt to get a track listing
                $trackListing = $this->_textService->createTrackListing( $tracksArray );
            }
        }

        if( !empty( $trackListing ) )
        {
            // append the track listing to the description
            $description .= "\n\n" . $trackListing;
        }

        return trim( $description );
    }

    /**
     * This function decides the product description by picking the one closest to 2000 characters in length
     *
     * @param array $processingProduct
     * @param int   $rootCategoryId
     * @param bool  $ignoreProductDescriptions
     *
     * @return mixed
     */
    public function defaultDescriptionDecision( array $processingProduct, $rootCategoryId, $ignoreProductDescriptions )
    {
        $this->debugMessage( "Using default description selector" );
        $potentialDescriptionArray = array();

        $this->debugMsg( json_encode( $processingProduct ) );
        //if an adminUser has set the description use only that one
        if( isset( $processingProduct['product_description'] ) )
        {
            foreach( $processingProduct['product_description'] as $dataProviderProductIdentifier => $dataProviderProductDescription )
            {
                // Look for straight DPP descriptions
                if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 36 )
                {
                    $this->debugMsg( json_encode( $processingProduct ) );
                    $this->debugMsg( 'check fixturesw' );
                    // if we have a description DPP and this is a database one then skip it
                    $this->addPotentialDescription( $potentialDescriptionArray, $dataProviderProductIdentifier, $dataProviderProductDescription, 'description' );

                    return reset( $potentialDescriptionArray )['description'];
                }
            }

            foreach( $processingProduct['product_description'] as $dataProviderProductIdentifier => $dataProviderProductDescription )
            {
                // Look for straight DPP descriptions
                if( $ignoreProductDescriptions && explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 0 )
                {
                    // if we have a description DPP and this is a database one then skip it
                    continue;
                }
                $this->addPotentialDescription( $potentialDescriptionArray, $dataProviderProductIdentifier, $rootCategoryId, $dataProviderProductDescription, 'description' );
            }
        }

        if( isset( $processingProduct['product_attributes']['wikipedia']['Introduction'] ) )
        {
            foreach( $processingProduct['product_attributes']['wikipedia']['Introduction'] as $dataProviderProductIdentifier => $dataProviderProductWiki )
            {
                // look for wikipedia introductions
                if( $ignoreProductDescriptions && explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 0 )
                {
                    // if we have a description DPP and this is a database one then skip it
                    continue;
                }
                $this->addPotentialDescription( $potentialDescriptionArray, $dataProviderProductIdentifier, $rootCategoryId, $dataProviderProductWiki, 'wiki_introduction' );

            }
        }

        if( isset( $processingProduct['product_attributes']['product_description'] ) )
        {
            foreach( $processingProduct['product_attributes']['product_description'] as $dataProviderProductIdentifier => $dataProviderProductDescriptionAttribute )
            {
                // look for product description attributes
                if( $ignoreProductDescriptions && explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 0 )
                {
                    // if we have a description DPP and this is a database one then skip it
                    continue;
                }

                if( is_string( $dataProviderProductDescriptionAttribute ) )
                {
                    $this->addPotentialDescription(
                        $potentialDescriptionArray,
                        $dataProviderProductIdentifier,
                        $rootCategoryId,
                        $dataProviderProductDescriptionAttribute,
                        'description_attribute'
                    );
                }
            }
        }

        if( isset( $processingProduct['product_attributes']['about'] ) && isset( $processingProduct['product_attributes']['about']['description'] ) )
        {
            foreach( $processingProduct['product_attributes']['about']['description'] as $dataProviderProductIdentifier => $dataProviderProductAboutAttribute )
            {
                // look for product about attributes
                if( $ignoreProductDescriptions && explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] == 0 )
                {
                    // if we have a description DPP and this is a database one then skip it
                    continue;
                }
                $this->addPotentialDescription( $potentialDescriptionArray, $dataProviderProductIdentifier, $rootCategoryId, $dataProviderProductAboutAttribute, 'about' );
            }
        }

        // sort the gathered descriptions by their differences in length from the ideal (as this is the first array field)
        asort( $potentialDescriptionArray );

        $this->debugMessage( json_encode( $potentialDescriptionArray ), 3 );

        return reset( $potentialDescriptionArray )['description'];
    }

    /**
     * this function cleans descriptions and prepares them and metadata about them so a decision can be made
     *
     * @param array  $potentialDescriptionArray
     * @param int    $dataProviderProductIdentifier
     * @param int    $rootCategoryId
     * @param string $description
     * @param string $source
     */
    private function addPotentialDescription( array &$potentialDescriptionArray, $dataProviderProductIdentifier, $rootCategoryId, $description, $source = '' )
    {
        $this->debugMessage(
            "getting description from : $dataProviderProductIdentifier" . ( empty( $source ) ?
                '' :
                ( self::SOURCE_IDENTIFIER_DELIMITER . $source ) ),
            2
        );

        // strip things we don't want form the description
        $potentialDescription = $this->_textService->cleanDataProviderProductDescription(
            explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0],
            $description,
            $rootCategoryId
        );

        $descriptionId = md5( $potentialDescription );

        if( isset( $potentialDescriptionArray[$descriptionId] ) )
        {
            // if it's already in the array just add a DPP origin to it
            $potentialDescriptionArray[$descriptionId]['data_providers'][] = $dataProviderProductIdentifier;
        }
        else
        {
            $effectiveLength = strlen( preg_replace( "/' id='\d+'\]/", '', str_replace( "[iLink name='", '', $potentialDescription ) ) );
            $this->debugMsg( $potentialDescription );

            $effectiveLength = strlen( preg_replace( '/[^a-zA-Z]/', '', strip_tags( preg_replace( "/\[iLink name='(.*?)' id='\d+'\]/", '$1', $potentialDescription ) ) ) );
            if( $effectiveLength >= 24 )
            {
                // else add it to the array of potential descriptions
                $potentialDescriptionArray[$descriptionId] = array(
                    'length_difference' => abs( 65000 - $effectiveLength ) * ceil( 400 / $effectiveLength ),
                    'description'       => $potentialDescription,
                    'data_providers'    => array(
                        $dataProviderProductIdentifier . ( empty( $source ) ?
                            '' :
                            ( self::SOURCE_IDENTIFIER_DELIMITER . $source ) )
                    ),
                );
            }
        }
        $this->debugMsg( $potentialDescriptionArray );

        $this->debugMessage( "description : $potentialDescription", 3 );
    }

    /**
     * function creates a description for music artists out of their discography
     *
     * @param $product
     *
     * @return string
     */
    private function musicArtistDiscographyDescription( $product )
    {
        $productMediaTypeArray = array();
        foreach( $product['product_attributes'] as $productAttributeTypeName => $productAttributeDataProviders )
        {
            // if the attribute is a music_album or music_single
            if( $productAttributeTypeName == 'album_id' || $productAttributeTypeName == 'single_id' )
            {
                $mediaProductIdsArray = array();
                foreach( $product['product_attributes']['album_id'] as $productAttributeDataProviderIdentifier => $productAttributeValue )
                {
                    if( explode( ProductAssemblerService::SOURCE_IDENTIFIER_DELIMITER, $productAttributeDataProviderIdentifier )[0] )
                    {
                        $mediaProductIdsArray = array_unique( array_merge( $mediaProductIdsArray, $productAttributeValue ) );
                    }
                }

                if( !empty( $mediaProductIdsArray ) )
                {
                    foreach( $mediaProductIdsArray as $mediaProductId )
                    {
                        $productMediaTypeArray[$mediaProductId] = 'album';
                    }
                }
            }

            if( isset( $product['product_attributes']['single_id'] ) && !empty( $product['product_attributes']['single_id'] ) )
            {
                $mediaProductIdsArray = array();
                foreach( $product['product_attributes']['single_id'] as $productAttributeDataProviderIdentifier => $productAttributeValue )
                {
                    if( explode( ProductAssemblerService::SOURCE_IDENTIFIER_DELIMITER, $productAttributeDataProviderIdentifier )[0] )
                    {
                        $mediaProductIdsArray = array_unique( array_merge( $mediaProductIdsArray, $productAttributeValue ) );
                    }
                }

                if( !empty( $mediaProductIdsArray ) )
                {
                    foreach( $mediaProductIdsArray as $mediaProductId )
                    {
                        $productMediaTypeArray[$mediaProductId] = 'single';
                    }
                }
            }
        }

        if( !empty( $productMediaTypeArray ) )
        {
            return $this->_textService->createDescriptionFromDiscography( $productMediaTypeArray );
        }
        else
        {
            return '';
        }

    }

    private function tvSubproductDescriptionDecision( $processingProduct, $rootCategoryId, $hasDataProviderProductDescription, $productId )
    {
        $tmdbProductIdentifier = null;
        $iTunesProductIdentifier = null;

        if( !isset( $processingProduct['product_description'] ) )
        {
            foreach( $processingProduct['product_description'] as $descriptionProductIdentifier => $description )
            {
                $dataProviderId = explode( self::SOURCE_IDENTIFIER_DELIMITER, $descriptionProductIdentifier )[0];
                if( $dataProviderId == $this->_dataProviderIdsArray[TMDbDataProvider::TMDB_DATA_PROVIDER_NAME] )
                {
                    $tmdbProductIdentifier = $descriptionProductIdentifier;
                }
                elseif( $dataProviderId == $this->_dataProviderIdsArray[ItunesDataProvider::ITUNES_DATA_PROVIDER_NAME] )
                {
                    $iTunesProductIdentifier = $descriptionProductIdentifier;
                }
            }
        }

        if( $iTunesProductIdentifier && $tmdbProductIdentifier )
        {
            $parentTmdbDescription = $this->_db->createCommand( "
SELECT product_description
FROM data_provider_product AS dpp
JOIN product_subproduct AS ps ON dpp.product_id = ps.product_id
WHERE dpp.data_provider_id = :tmdb
AND ps.subproduct_id = :prdId;" )
                                               ->bindValue( ':tmdb', $this->_dataProviderIdsArray[TMDbDataProvider::TMDB_DATA_PROVIDER_NAME] )
                                               ->bindValue( ':prdId', $productId )
                                               ->queryScalar();

            if( $parentTmdbDescription == $processingProduct['product_description'][$tmdbProductIdentifier] )
            {
                unset( $processingProduct['product_description'][$tmdbProductIdentifier] );
            }
        }

        return $this->defaultDescriptionDecision( $processingProduct, $rootCategoryId, $hasDataProviderProductDescription );
    }

    /**
     * Function decides a release date by picking the lowest valid release date
     *
     * @param $rawProduct
     *
     * @return int
     */
    public function decideReleaseDate( $rawProduct )
    {
        $this->debugMessage( "Deciding release date" );
        if( !empty( $rawProduct['release_date'] ) )
        {
            $releaseDateProducts = array();
            foreach( $rawProduct['release_date'] as $dataProviderProductIdentifier => $dataProviderProductReleaseDate )
            {
                if( strlen( $dataProviderProductReleaseDate ) == 8 )
                {
                    $invalidDateData = false;
                    //if the length is 8, and it is a date
                    $dateCheck = array(
                        'year'  => substr( $dataProviderProductReleaseDate, 0, 4 ),
                        'month' => ( substr( $dataProviderProductReleaseDate, 4, 2 ) == '00' ? '01' : substr( $dataProviderProductReleaseDate, 4, 2 ) ),
                        'day'   => ( substr( $dataProviderProductReleaseDate, 6, 2 ) == '00' ? '01' : substr( $dataProviderProductReleaseDate, 6, 2 ) ),
                    );

                    foreach( $dateCheck as $datePart => $datePartValue )
                    {
                        if( intval( $datePartValue ) === 0 )
                        {
                            $invalidDateData = true;
                        }
                        else
                        {
                            $dateCheck[$datePart] = str_pad( intval( $datePartValue ), 2, 0, STR_PAD_LEFT );
                        }
                    }

                    if( !$invalidDateData )
                    {
                        if( checkdate( $dateCheck['month'], $dateCheck['day'], $dateCheck['year'] ) )
                        {
                            $releaseDateProducts['uk'][] = $dateCheck['year'] . $dateCheck['month'] . $dateCheck['day'];
                        }
                    }
                }
            }
        }
        if( !empty( $rawProduct['product_attributes']['release_date'] ) )
        {
            $releaseDateDataProviderProducts = array();
            foreach( $rawProduct['product_attributes']['release_date'] as $dataProviderProductIdentifier => $dataProviderProductAttributeReleaseDate )
            {
                if( is_string( $dataProviderProductAttributeReleaseDate ) )
                {
                    if( explode( '::', $dataProviderProductIdentifier )[0] == 0 )
                    {
                        $releaseDateProducts['uk'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate ) );
                    }
                    else
                    {
                        $releaseDateDataProviderProducts['uk'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate ) );
                    }
                }
                elseif( is_array( $dataProviderProductAttributeReleaseDate ) && isset ( $dataProviderProductAttributeReleaseDate['uk'] ) )
                {
                    if( explode( '::', $dataProviderProductIdentifier )[0] == 0 )
                    {
                        $releaseDateProducts['uk'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate['uk'] ) );
                    }
                    else
                    {
                        $releaseDateDataProviderProducts['uk'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate['uk'] ) );
                    }
                }
                elseif( is_array( $dataProviderProductAttributeReleaseDate ) && isset ( $dataProviderProductAttributeReleaseDate[0]['uk'] ) )
                {
                    if( explode( '::', $dataProviderProductIdentifier )[0] == 0 )
                    {
                        $releaseDateProducts['uk'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate[0]['uk'] ) );
                    }
                    else
                    {
                        $releaseDateDataProviderProducts['uk'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate[0]['uk'] ) );
                    }
                }
                elseif( is_array( $dataProviderProductAttributeReleaseDate ) )
                {
                    while( is_array( $dataProviderProductAttributeReleaseDate ) )
                    {
                        $dataProviderProductAttributeReleaseDate = reset( $dataProviderProductAttributeReleaseDate );
                    }
                    if( is_string( $dataProviderProductAttributeReleaseDate ) )
                    {
                        if( explode( '::', $dataProviderProductIdentifier )[0] == 0 )
                        {
                            $releaseDateProducts['other'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate ) );
                        }
                        else
                        {
                            $releaseDateDataProviderProducts['other'][] = date( 'Ymd', strtotime( $dataProviderProductAttributeReleaseDate ) );
                        }
                    }
                }
            }
        }
        $this->checkReleaseDateRange( $releaseDateProducts );
        $this->checkReleaseDateRange( $releaseDateDataProviderProducts );

        $releaseDate = 0;
        if( !empty( $releaseDateDataProviderProducts['uk'] ) )
        {
            sort( $releaseDateDataProviderProducts['uk'] );
            $releaseDate = reset( $releaseDateDataProviderProducts['uk'] );
        }
        elseif( !empty( $releaseDateDataProviderProducts['other'] ) )
        {
            sort( $releaseDateDataProviderProducts['other'] );
            $releaseDate = reset( $releaseDateDataProviderProducts['other'] );
        }
        elseif( !empty( $releaseDateProducts['uk'] ) )
        {
            sort( $releaseDateProducts['uk'] );
            $releaseDate = reset( $releaseDateProducts['uk'] );
        }
        elseif( !empty( $releaseDateProducts['other'] ) )
        {
            sort( $releaseDateProducts['other'] );
            $releaseDate = reset( $releaseDateProducts['other'] );
        }

        return $releaseDate;
    }

    /**
     * Checks if a date is in a specified range, unsets it if not
     *
     * @param $dateArrays array of dates in YYYYMMDD format
     */
    private function checkReleaseDateRange( &$dateArrays )
    {
        //so far there's only two sub arrays, uk and other
        $dateKeysArray = array('uk', 'other');
        foreach( $dateKeysArray as $dateTypeKeys )
        {
            if( isset( $dateArrays[$dateTypeKeys] ) )
            {
                foreach( $dateArrays[$dateTypeKeys] as $key => $value )
                {
                    //all dates should be in the range -400 years to +2 years from now
                    $comparisonDate = ( date( 'Y' ) + 1 ) . '0000';

                    $difference = ( $value - $comparisonDate ) / 10000;
                    if( $difference < -400 || $difference >= 3 )
                    {
                        unset( $dateArrays[$dateTypeKeys][$key] );
                    }
                }
            }
        }
    }

    /**
     * This function controls what is selected for each product attribute
     *
     * @param array $processingProduct
     * @param int   $rootCategoryId
     *
     * @return array
     */
    public function decideProductAttributeArray( array $processingProduct, $rootCategoryId )
    {
        $finalProductAttributeArray = array();
        foreach( $processingProduct['product_attributes'] as $attributeTypeName => $processingProductAttribute )
        {
            $this->debugMessage( "Deciding product attribute : $attributeTypeName" );

            // check for DPP versions of this attribute
            $hasDataProviderProductAttribute = false;
            foreach( $processingProductAttribute as $dataProviderProductIdentifier => $dataProviderProductAttribute )
            {
                if( preg_match( "/^\d\d?::.+$/", $dataProviderProductIdentifier ) )
                {
                    // if it's DPPIdentifier => value
                    if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $dataProviderProductIdentifier )[0] != 0 )
                    {
                        $hasDataProviderProductAttribute = true;
                        break;
                    }
                }
                else
                {
                    // if it's subAttributeName => DPPIdentifier => value
                    foreach( $dataProviderProductAttribute as $subDataProviderProductIdentifier => $dataProviderProductSubAttribute )
                    {
                        if( explode( self::SOURCE_IDENTIFIER_DELIMITER, $subDataProviderProductIdentifier )[0] != 0 )
                        {
                            $hasDataProviderProductAttribute = true;
                            break;
                        }
                    }
                }
            }
            $this->debugMessage(
                "Product " . ( ( $hasDataProviderProductAttribute ) ?
                    'has' :
                    'does not have' ) . " data providers for this attribute",
                2
            );

            // get the attribute value
            switch( $rootCategoryId )
            {
                default:
                    $finalAttribute = $this->defaultDecideProductAttribute( $processingProductAttribute, $hasDataProviderProductAttribute );
            }

            // if it isn't empty add it to the final product
            if( !empty( $finalAttribute ) )
            {
                $finalProductAttributeArray[$attributeTypeName] = $finalAttribute;
            }
        }

        return $finalProductAttributeArray;
    }

    /*
     * This function updates a master product's user interests so that any that are missing or more recent among its subordinates are used instead
     *
     * @todo: think of a way to implement this that doesn't make it impossible to delete your interests (maybe change the delete user interest api to also delete interests from subordinate products?)
     *
     * @param int   $masterProductId
     * @param array $subordinateProductIdsArray
     */
#    private function updateMasterUserInterest( $masterProductId, array $subordinateProductIdsArray )
#    {
#        $this->debugMessage( "Updating interest" );
#        // get the current user interest of the master
#        $masterInterestSQL = "SELECT user_id, last_updated FROM user_interest WHERE product_id = $masterProductId;";
#
#        $masterInterestResult = $this->_db->createCommand( $masterInterestSQL )
#                                          ->queryAll();
#
#        $masterInterestUserIdsArray = array_column( $masterInterestResult, 'user_id' );
#
#        $masterInterestLastUpdatedArray = array_column( $masterInterestResult, 'last_updated', 'user_id' );
#
#        $this->debugMessage( "Current interested user ids : " . implode( ', ', $masterInterestUserIdsArray ), 2 );
#
#        // get the most resent interest among the subordinates
#        $subordinateInterestSQL = "SELECT user_id, last_updated, interested FROM user_interest WHERE product_id IN (" . implode( ',', $subordinateProductIdsArray ) . ")
#                                   ORDER BY user_id, last_updated ASC;";
#
#        $subordinateInterestResult = $this->_db->createCommand( $subordinateInterestSQL )
#                                               ->queryAll();
#
#       // extract a simple list of user ids of interest
#      $subordinateInterestUserIdsArray = array_column( $subordinateInterestResult, 'user_id', 'user_id' );
#     $this->debugMessage( "desired interested user ids : " . implode( ', ', $subordinateInterestUserIdsArray ), 2 );
#
#       // get an associative array of last_updated of user interest
#      $subordinateInterestLastUpdatedArray = array_column( $subordinateInterestResult, 'last_updated', 'user_id' );
#     $subordinateInterestInterestedArray = array_column( $subordinateInterestResult, 'interested', 'user_id' );
#    $this->debugMessage( "interest last updated : " . json_encode( $subordinateInterestLastUpdatedArray ), 3 );
#
#       // unlike related products/categories/images we don't want to remove excess interest as it's possible to rate the master product directly
#
#       // get a list of products that need to be added
#      $missingInterestUserIdsArray = array_diff( $subordinateInterestUserIdsArray, $masterInterestUserIdsArray );
#
#       if( !empty( $missingInterestUserIdsArray ) )
#      {
#         $this->debugMessage( "Adding missing user interest", 2 );
#        $this->debugMessage( implode( ', ', $missingInterestUserIdsArray ), 3 );
#       /**
#       * @var UserInterest[] $newUserInterestArray
#      */
#    $newUserInterestArray = array();
#
#           // add each missing rating
#          foreach( $missingInterestUserIdsArray as $missingInterestUserId )
#         {
#            $newUserInterestArray[$missingInterestUserId] = new UserInterest();
#           $newUserInterestArray[$missingInterestUserId]->setAttributes( array(
#              'user_id'      => $missingInterestUserId,
#             'product_id'   => $masterProductId,
#            'interested'   => $subordinateInterestInterestedArray[$missingInterestUserId],
#           'last_updated' => $subordinateInterestLastUpdatedArray[$missingInterestUserId]
#      ) );
#     $newUserInterestArray[$missingInterestUserId]->save();
#}
#        }
#
#       // get a list of interest that need to be updated because there is a subordinate that has had its rating updated more recently than the master's
#      $outOfDateInterestLastUpdatedArray = array_udiff_assoc( array_intersect_key( $subordinateInterestLastUpdatedArray, $masterInterestLastUpdatedArray ), $masterInterestLastUpdatedArray, function ( $a, $b )
#     {
#        return $a > $b;
#   } );
#
#       if( !empty( $outOfDateInterestLastUpdatedArray ) )
#      {
#         $this->debugMessage( "Updating out of date user interest", 2 );
#        $this->debugMessage( implode( ', ', array_keys( $outOfDateInterestLastUpdatedArray ) ), 3 );
#       /**
#       * @var UserInterest[] $updatedUserInterestArray
#      */
#    $updatedUserInterestArray = array();
#   foreach( $outOfDateInterestLastUpdatedArray as $outOfDateInterestUserId => $outOfDateInterestNewLastUpdated )
#  {
#     // find each out of date related product
#    if( $updatedUserInterestArray[$outOfDateInterestUserId] = UserInterest::model()
#                                                                         ->findByAttributes( array(
#                                                                            'product_id' => $masterProductId,
#                                                                           'user_id'    => $outOfDateInterestUserId,
#                                                                      ) )
#                )
#               {
#                  // update it
#                 $updatedUserInterestArray[$outOfDateInterestUserId]->setAttributes( array(
#                    'last_updated' => $outOfDateInterestNewLastUpdated,
#                   'interested'   => $subordinateInterestInterestedArray[$outOfDateInterestUserId]
#              ) );
#             $updatedUserInterestArray[$outOfDateInterestUserId]->save();
#        }
#   }
#        }
#   }

    /**
     * default function to decide an attribute's value.
     *
     * @param array $processingProductAttribute
     * @param bool  $ignoreProductAttribute
     *
     * @return string
     */
    public function defaultDecideProductAttribute( array $processingProductAttribute, $ignoreProductAttribute )
    {
        $potentialAttributes = array();
        $combinedAttribute = array();
        foreach( $processingProductAttribute as $attributeKey => $attributeValue )
        {
            if( !empty( $attributeValue ) )
            {
                if( preg_match( "/^\d\d?::.+$/", $attributeKey ) )
                {
                    // if it's DPPIdentifier => value
                    if( $ignoreProductAttribute && explode( self::SOURCE_IDENTIFIER_DELIMITER, $attributeKey )[0] == 0 )
                    {
                        // if we have a DPP value for this attribute and this is a database value skip it
                        continue;
                    }

                    // if the value's a string take it as is, if it's an array json_encode it
                    $potentialAttribute = is_array( $attributeValue ) ?
                        json_encode( $attributeValue ) :
                        $attributeValue;

                    $attributeId = md5( $potentialAttribute );
                    if( isset( $potentialAttributes[$attributeId] ) )
                    {
                        // if we have this attribute already add the data_provider to it's list
                        $potentialAttributes[$attributeId]['data_provider_count'] += 1;
                        $potentialAttributes[$attributeId]['data_providers'][] = $attributeKey;
                    }
                    else
                    {
                        // if it's the first time we've seen this value add it to the array formatted so it'll be ordered by the number of times it's been seen then it's length
                        $potentialAttributes[$attributeId] = array(
                            'data_provider_count' => 1,
                            'length'              => strlen( $potentialAttribute ),
                            'attribute'           => $potentialAttribute,
                            'data_providers'      => array($attributeKey),
                        );
                    }
                }
                else
                {
                    $potentialSubAttributes = array();
                    foreach( $attributeValue as $subAttributeKey => $subAttributeValue )
                    {
                        // if it's in the format subAttributeName => DPPIdentifier => value
                        if( $ignoreProductAttribute && explode( self::SOURCE_IDENTIFIER_DELIMITER, $subAttributeKey )[0] == 0 )
                        {
                            // if we have a DPP value and this is a database value then skip it
                            continue;
                        }

                        // even if it's an array it will be encoded later so don't right now
                        $potentialSubAttribute = $subAttributeValue;

                        $subAttributeId = md5( json_encode( $potentialSubAttribute ) );
                        if( isset( $potentialSubAttributes[$subAttributeId] ) )
                        {
                            // if we have this attribute already add the data_provider to it's list
                            $potentialSubAttributes[$subAttributeId]['data_provider_count'] += 1;
                            $potentialSubAttributes[$subAttributeId]['data_providers'][] = $subAttributeKey;
                        }
                        else
                        {
                            // if it's the first time we've seen this value add it to the array formatted so it'll be ordered by the number of times it's been seen then it's length
                            $potentialSubAttributes[$subAttributeId] = array(
                                'data_provider_count' => 1,
                                'length'              => strlen( json_encode( $potentialSubAttribute ) ),
                                'attribute'           => $potentialSubAttribute,
                                'data_providers'      => array($subAttributeKey),
                            );
                        }
                    }
                    // select the 'best' value for this sub attribute
                    arsort( $potentialSubAttributes );
                    $combinedAttribute[$attributeKey] = reset( $potentialSubAttributes )['attribute'];
                }
            }
        }
        if( !empty( array_filter( $potentialAttributes ) ) )
        {
            // if we don't have sub attributes pick the 'best' value
            arsort( $potentialAttributes );

            // return the best value
            return reset( $potentialAttributes )['attribute'];
        }
        elseif( !empty( array_filter( $combinedAttribute ) ) )
        {
            // if we have sub attributes they'll have already been selected so just return a json encode of them
            return json_encode( $combinedAttribute );
        }

        return null;
    }

    /**
     * Function acts as as container to call other functions to
     * make sure that a Master product looks like its subordinates
     *
     * @param $masterProductId
     * @param $masterRootCategoryId
     */
    public function updateMasterProductPeripheralInformation( $masterProductId, $masterRootCategoryId )
    {
        $this->debugMessage( "Updating peripheral information for master product : $masterProductId" );
        // get a list of subordinate product ids
        if( !$validSubordinateProductIdsArray = $this->getValidSubordinateProductIds( $masterProductId ) )
        {
            // bail if there are none
            return;
        }

        // update the categories of the master product to reflect those of the subordinates
        $this->updateMasterProductCategories( $masterProductId, $masterRootCategoryId, $validSubordinateProductIdsArray );

        // update the images of the master product to include those of the subordinates
        $this->updateMasterProductImages( $masterProductId, $validSubordinateProductIdsArray );

        // update the related products of the master product to take account of those of the subordinates
        $this->updateMasterProductRelatedProducts( $masterProductId, $validSubordinateProductIdsArray );

        // update the ecommerce links of the master product to reflect those of the subordinate products
        $this->updateMasterProductEcommerceProducts( $masterProductId, $masterRootCategoryId, $validSubordinateProductIdsArray );

        /*
         * @todo: subproducts (leave for now as it doesn't apply to books/games yet)
         */

        // update the ratings of the master product (this won't ever delete ratings)
        $this->updateMasterProductRatings( $masterProductId, $masterRootCategoryId, $validSubordinateProductIdsArray );
        $this->updateMasterProductReviews( $masterProductId, $masterRootCategoryId, $validSubordinateProductIdsArray );
        $this->_productService->updateProductReviewNumbers( $masterProductId );

        // update the user interest in the master product (this isn't working properly yet)
        #$this->updateMasterUserInterest( $masterProductId, $validSubordinateProductIdsArray );

        $this->debugMessage( "finished updating peripheral information\n" );
    }

    /**
     * This function gets a list of valid subordinate product ids for a master product
     *
     * @param $masterId
     *
     * @return array|bool
     */
    public function getValidSubordinateProductIds( $masterId )
    {
        $this->debugMessage( "Finding valid subordinates for : $masterId" );
        if( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationDataProvider' ) )
        {
        // get the PDDPP for the master product
        $masterSubordinateIdsSQL = "SELECT product_attributes FROM data_provider_product WHERE product_id = :masterId AND product_name = :productName AND data_provider_id = :dataProviderId;";

        // get the attributes (json decoded) of the PDDPP
        $masterSubordinateIdsResult = json_decode(
            $this->_db->createCommand( $masterSubordinateIdsSQL )
                      ->queryScalar(
                          array(
                              ':masterId'       => $masterId,
                              ':productName'    => ProductDuplicationDataProvider::MASTER_PRODUCT_NAME,
                              ':dataProviderId' => $this->_dataProviderIdsArray['productDuplication']
                          )
                      ),
            true
        );

        if( !empty( $masterSubordinateIdsResult[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
        {
            // get the array of subordinate ids
            $masterSubordinateIdsArray = $masterSubordinateIdsResult[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];

            // get the PDDPPs for the subordinates (and only subordinates, not conflicted ones)
            $subordinatesMasterIdsSQL = "SELECT product_id, product_attributes FROM data_provider_product WHERE product_id IN (" . implode( ',', $masterSubordinateIdsArray ) . ")
                                        AND product_name = :productName AND data_provider_id = :dataProviderId;";

            $subordinatesMasterIdsResult = $this->_db->createCommand( $subordinatesMasterIdsSQL )
                                                     ->queryAll(
                                                         true,
                                                         array(
                                                             ':productName'    => ProductDuplicationDataProvider::SUBORDINATE_PRODUCT_NAME,
                                                             ':dataProviderId' => $this->_dataProviderIdsArray['productDuplication'],
                                                         )
                                                     );

            // if we got some subordinates (they might have all been conflicted
            if( !empty( $subordinatesMasterIdsResult ) )
            {
                $validSubordinateProductIds = array();
                foreach( $subordinatesMasterIdsResult as $subordinatesMasterId )
                {
                    $this->debugMessage( "Checking " . $subordinatesMasterId['product_id'] . " is a valid subordinate", 2 );
                    // check each subordinate is set as a subordinate of the master
                    if( ( $attributes = json_decode( $subordinatesMasterId['product_attributes'], true ) )
                        && isset( $attributes[ProductDuplicationDataProvider::SUBORDINATE_MASTER_PRODUCT_ID_KEY] )
                        && $attributes[ProductDuplicationDataProvider::SUBORDINATE_MASTER_PRODUCT_ID_KEY] == $masterId
                    )
                    {
                        // if so add its product id to the array of valid ida
                        $validSubordinateProductIds[] = $subordinatesMasterId['product_id'];
                    }

                }

                $this->debugMessage( "Found " . count( $validSubordinateProductIds ) . " valid subordinate products" );

                return $validSubordinateProductIds;
            }
        }

        return false;
    }
        elseif( is_a( $this->_productDuplicationDataProvider, 'ProductDuplicationService' ) )
        {
            // get all product relations for the master product
            $masterProductRelations = $this->_productDuplicationDataProvider->getProductRelations( $masterId );

            if( !empty( $masterProductRelations[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
            {
                // get the array of subordinate ids
                $masterSubordinateIdsArray = $masterProductRelations[ProductDuplicationService::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];

                // get the PDDPPs for the subordinates (and only subordinates, not conflicted ones)
                foreach( $masterSubordinateIdsArray as $subordinateProductId )
                {
                    $subordinateProductRelations = $this->_productDuplicationDataProvider->getProductRelations( $subordinateProductId );

                    // if we got some subordinates (they might have all been conflicted
                    if( isset( $subordinateProductRelations[ProductDuplicationService::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) && !empty( $subordinateProductRelations[ProductDuplicationDataProvider::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) && $subordinateProductRelations[ProductDuplicationDataProvider::SUBORDINATE_MASTER_PRODUCT_ID_KEY][0] == $masterId )
                    {
                        // if so add its product id to the array of valid ida
                        $validSubordinateProductIds[] = $subordinateProductId;
                    }
                }

                $this->debugMessage( "Found " . count( $validSubordinateProductIds ) . " valid subordinate products" );

                return $validSubordinateProductIds;

            }

            return false;
        }
    }

    /**
     * This function updates Master products so their categories are the same as their duplicates' categories
     *
     * @param       $masterProductId
     * @param       $masterRootCategoryId
     * @param array $subordinateProductIdsArray
     */
    private function updateMasterProductCategories( $masterProductId, $masterRootCategoryId, array $subordinateProductIdsArray )
    {
        $this->debugMessage( "Updating categories" );
        // get array of current category Ids of the master product
        $masterCategoryIds = $this->_db->createCommand( "SELECT category_id FROM category_product WHERE product_id = :mstrId" )
                                       ->bindValue( ':mstrId', $masterProductId )
                                       ->queryColumn();

        $subordinateProductIdBindings = Utilities::createQueryBindings( $subordinateProductIdsArray, 'subId' );
        if( $subordinateCategoryIds = $this->_db->createCommand(
            "
SELECT DISTINCT cs.ancestor_category_id
FROM category_product AS cp
JOIN category AS c ON cp.category_id = c.category_id
JOIN category_subcategory AS cs ON cs.descendant_category_id = cp.category_id
WHERE cp.product_id IN (" . implode( ',', array_keys( $subordinateProductIdBindings ) ) . ")
AND c.root_category_id = :mstrRtId;"
        )
                                                ->bindValues( $subordinateProductIdBindings )
                                                ->bindValue( ":mstrRtId", $masterRootCategoryId )
                                                ->queryColumn()
        )
        {
            $subordinateCategoryIdBindings = Utilities::createQueryBindings( $subordinateCategoryIds, 'subCatId' );
            $subordinateLeafCategoryIds = $this->_db->createCommand(
                "SELECT ancestor_category_id
FROM category_subcategory
WHERE ancestor_category_id IN (" . implode( ',', array_keys( $subordinateCategoryIdBindings ) ) . ")
AND descendant_category_id IN (" . implode( ',', array_keys( $subordinateCategoryIdBindings ) ) . ")
GROUP BY ancestor_category_id
HAVING COUNT(*) = 1;"
            )
                                                    ->bindValues( $subordinateCategoryIdBindings )
                                                    ->queryColumn();
        }
        else
        {
            $subordinateLeafCategoryIds = [$masterRootCategoryId];
        }
        $changesMade = false;
        if( $excessCategoryIds = array_diff( $masterCategoryIds, $subordinateCategoryIds ) )
        {
            $changesMade = true;
            $excessCategoryIdBindings = Utilities::createQueryBindings( $excessCategoryIds, "exCatId" );
            $this->_db->createCommand(
                "DELETE FROM category_product WHERE product_id = :mstrId AND category_id IN (" . implode( ',', array_keys( $excessCategoryIdBindings ) ) . ");"
            )
                      ->bindValue( ':mstrId', $masterProductId )
                      ->bindValues( $excessCategoryIdBindings )
                      ->execute();
        }

        if( $missingCategoryIds = array_diff( $subordinateLeafCategoryIds, $masterCategoryIds ) )
        {
            $changesMade = true;
            list( $missingCategoryIdBindingLocations, $missingCategoryIdBindings ) = Utilities::createSingleKeyInsertBindings(
                array_fill_keys( $missingCategoryIds, $masterProductId )
            );
            $this->_db->createCommand( "INSERT IGNORE INTO category_product (category_id, product_id) VALUES " . implode( ',', $missingCategoryIdBindingLocations ) )
                      ->bindValues( $missingCategoryIdBindings )
                      ->execute();
        }

        if( $changesMade )
        {
            $categoryProductService = new CategoryProductService();
            $categoryProductService->updateRelevanceForProduct( $masterProductId );
        }
    }

    /**
     * This function updates Master products so their images are the same as their duplicates' images
     *
     * @param       $masterProductId
     * @param array $subordinateProductIdsArray
     */
    private function updateMasterProductImages( $masterProductId, array $subordinateProductIdsArray )
    {
        $this->debugMessage( "Updating images" );
        // get array of current image urls of the master product
        $masterImageUrlsSQL = "SELECT image_url FROM product_image WHERE product_id = :productId;";

        $masterProductUrlsArray = $this->_db->createCommand( $masterImageUrlsSQL )
                                            ->queryColumn(
                                                array(
                                                    ':productId' => $masterProductId
                                                )
                                            );

        $this->debugMessage( "Current images : " . implode( '  ', $masterProductUrlsArray ), 2 );

        // get details of all category ids of subordinate products
        $subordinateImagesSQL = "SELECT image_width, image_height, image_type, image_url FROM product_image
                                WHERE product_id IN (" . implode( ',', $subordinateProductIdsArray ) . ");";

        $subordinateImagesResult = $this->_db->createCommand( $subordinateImagesSQL )
                                             ->queryAll();

        // extract simple array of image urls of subordinate products
        $subordinateImageUrlsArray = array_column( $subordinateImagesResult, 'image_url' );
        $this->debugMessage( "desired images : " . implode( '  ', $subordinateImageUrlsArray ), 2 );

        // extract associative array of category relevance
        $subordinateImagesArray = array_combine( array_column( $subordinateImagesResult, 'image_url' ), $subordinateImagesResult );
        $this->debugMessage( "image details : " . json_encode( $subordinateImagesArray ), 3 );

        // get array of image urls that exist for the master but no duplicates ( and thus need to be removed )
        $excessImageUrlsArray = array_diff( $masterProductUrlsArray, $subordinateImageUrlsArray );

        if( !empty( $excessImageUrlsArray ) )
        {
            $this->debugMessage( "Deleting excess images", 2 );
            $this->debugMessage( implode( ', ', $excessImageUrlsArray ), 3 );
            // delete the excess images
            $deleteExcessProductImagesSQL = "DELETE FROM product_image WHERE product_id = :productId AND image_url IN ('" . implode( '","', $excessImageUrlsArray ) . "');";

            $this->_db->createCommand( $deleteExcessProductImagesSQL )
                      ->execute(
                          array(
                              ':productId' => $masterProductId
                          )
                      );
        }

        // get array of duplicates' images the are not on the master (and thus need to be added)
        $missingImageUrlsArray = array_diff( $subordinateImageUrlsArray, $masterProductUrlsArray );

        if( !empty( $missingImageUrlsArray ) )
        {
            $this->debugMessage( "Adding missing images", 2 );
            $this->debugMessage( implode( ', ', $missingImageUrlsArray ), 3 );

            /**
             * @var  ProductImage[] $newProductImagesArray
             */
            $newProductImagesArray = array();
            // create each missing image and save it
            foreach( $missingImageUrlsArray as $missingImageUrl )
            {
                $newProductImagesArray[$missingImageUrl] = new ProductImage();
                $newProductImagesArray[$missingImageUrl]->setAttributes(
                    array(
                        'image_url'     => $missingImageUrl,
                        'product_id'    => $masterProductId,
                        'image_height'  => $subordinateImagesArray[$missingImageUrl]['image_height'],
                        'image_width'   => $subordinateImagesArray[$missingImageUrl]['image_width'],
                        'image_type'    => $subordinateImagesArray[$missingImageUrl]['image_type'],
                        'image_url_md5' => hex2bin( md5( $subordinateImagesArray[$missingImageUrl]['image_url'] ) )
                    )
                );
                $newProductImagesArray[$missingImageUrl]->save();
            }
            $this->debugMessage( "Queue $masterProductId to the download images queue", 2 );
            $this->_messageQueue->queueMessage( AbstractDataProvider::IMAGE_DOWNLOAD_QUEUE_NAME, $masterProductId, true );
        }
    }

    /**
     * This function updates a master product's related products so they're the same as its duplicates' related products. It picks the 40 most relevant.
     *
     * @param       $masterProductId
     * @param array $subordinateProductIdsArray
     */
    private function updateMasterProductRelatedProducts( $masterProductId, array $subordinateProductIdsArray )
    {
        $this->debugMessage( "Updating related products" );
        // get the current related products of the master
        $masterRelatedProductIdsSQL = "SELECT related_product_id, relevancy_score FROM related_product WHERE product_id = :productId;";

        $masterRelatedProductIdsResult = $this->_db->createCommand( $masterRelatedProductIdsSQL )
                                                   ->queryAll(
                                                       true,
                                                       array(
                                                           ':productId' => $masterProductId
                                                       )
                                                   );

        $masterRelatedProductIdsArray = array_column( $masterRelatedProductIdsResult, 'related_product_id' );

        $masterRelatedProductRelevancyScoreArray = array_column( $masterRelatedProductIdsResult, 'relevancy_score', 'related_product_id' );

        $this->debugMessage( "Current related products : " . implode( ', ', $masterRelatedProductIdsArray ), 2 );

        // get the 40 most relevant products among the subordinates
        $subordinateRelatedProductIdsSQL = "SELECT rp.related_product_id,
                                  CAST(AVG(rp.relevancy_score) + 50 * count(DISTINCT rp.product_id) AS UNSIGNED ) AS relevancy_score
                                  FROM related_product rp
                                  JOIN product p ON (p.product_id = rp.related_product_id)
                                  JOIN category_product cp ON p.product_id = cp.product_id
                                  JOIN category c ON c.category_id = cp.category_id
                                  JOIN product_image pi ON pi.product_id = p.product_id
                                  WHERE rp.product_id IN (" . implode( ',', $subordinateProductIdsArray ) . ")
                                  AND rp.related_product_id NOT IN (" . implode( ',', $subordinateProductIdsArray ) . ")
                                  AND c.root_category_id IN(" . $this->getRootCategoriesCSV( 'GB' ) . ")
                                  AND p.display = 1 AND p.archived = 0
                                  GROUP BY rp.related_product_id
                                  ORDER BY relevancy_score DESC, p.product_weighted_rating DESC
                                  LIMIT 40;";

        $subordinateRelatedProductIdsResult = $this->_db->createCommand( $subordinateRelatedProductIdsSQL )
                                                        ->queryAll();

        // extract a simple list of ids of related products
        $subordinateRelatedProductIdsArray = array_column( $subordinateRelatedProductIdsResult, 'related_product_id', 'related_product_id' );
        $this->debugMessage( "desired related products : " . implode( ', ', $subordinateRelatedProductIdsArray ), 2 );

        // get an associative array of relevance of related products
        $subordinateRelatedProductRelevancyScoreArray = array_column( $subordinateRelatedProductIdsResult, 'relevancy_score', 'related_product_id' );
        $this->debugMessage( "related products relevancy score : " . json_encode( $subordinateRelatedProductRelevancyScoreArray ), 3 );

        // get a list of related products that need to be removed
        $excessRelatedProductIdsArray = array_diff( $masterRelatedProductIdsArray, $subordinateRelatedProductIdsArray );

        if( !empty( $excessRelatedProductIdsArray ) )
        {
            $this->debugMessage( "Deleting excess related products", 2 );
            $this->debugMessage( implode( ', ', $excessRelatedProductIdsArray ), 3 );
            // remove the excess products
            $deleteExcessRelatedProductsSQL = "DELETE FROM related_product WHERE product_id = :productId AND related_product_id IN (" . implode(
                    ',',
                    $excessRelatedProductIdsArray
                ) . ");";

            $this->_db->createCommand( $deleteExcessRelatedProductsSQL )
                      ->execute(
                          array(
                              ':productId' => $masterProductId
                          )
                      );
        }

        // get a list of products that need to be added
        $missingRelatedProductIdsArray = array_diff( $subordinateRelatedProductIdsArray, $masterRelatedProductIdsArray );

        if( !empty( $missingRelatedProductIdsArray ) )
        {
            $this->debugMessage( "Adding missing related products", 2 );
            $this->debugMessage( implode( ', ', $missingRelatedProductIdsArray ), 3 );
            /**
             * @var  RelatedProduct[] $newRelatedProductsArray
             */
            $newRelatedProductsArray = array();

            // add each missing related product
            foreach( $missingRelatedProductIdsArray as $missingRelatedProductId )
            {
                $newRelatedProductsArray[$missingRelatedProductId] = new RelatedProduct();
                $newRelatedProductsArray[$missingRelatedProductId]->setAttributes(
                    array(
                        'related_product_id' => $missingRelatedProductId,
                        'product_id'         => $masterProductId,
                        'relevancy_score'    => $subordinateRelatedProductRelevancyScoreArray[$missingRelatedProductId]
                    )
                );
                $newRelatedProductsArray[$missingRelatedProductId]->save();
            }
        }

        // get a list of products that need to be updated because their relevance has changed
        $outOfDateRelatedProductRelevancyScoreArray = array_diff_assoc(
            array_intersect_key( $subordinateRelatedProductRelevancyScoreArray, $masterRelatedProductRelevancyScoreArray ),
            $masterRelatedProductRelevancyScoreArray
        );

        if( !empty( $outOfDateRelatedProductRelevancyScoreArray ) )
        {
            $this->debugMessage( "Updating out of date related products", 2 );
            $this->debugMessage( implode( ', ', array_keys( $outOfDateRelatedProductRelevancyScoreArray ) ), 3 );
            /**
             * @var RelatedProduct[] $updatedRelatedProductArray
             */
            $updatedRelatedProductArray = array();
            foreach( $outOfDateRelatedProductRelevancyScoreArray as $outOfDateRelatedProductId => $outOfDateRelatedProductNewRelevancyScore )
            {
                // find each out of date related product
                if( $updatedRelatedProductArray[$outOfDateRelatedProductId] = RelatedProduct::model()
                                                                                            ->findByAttributes(
                                                                                                array(
                                                                                                    'product_id'         => $masterProductId,
                                                                                                    'related_product_id' => $outOfDateRelatedProductId,
                                                                                                )
                                                                                            )
                )
                {
                    // update it
                    $updatedRelatedProductArray[$outOfDateRelatedProductId]->setAttribute( 'relevancy_score', $outOfDateRelatedProductNewRelevancyScore );
                    $updatedRelatedProductArray[$outOfDateRelatedProductId]->save();
                }
            }
        }

        $updateMasterRelatedProductCountSQL = "UPDATE product SET num_related_items = (SELECT COUNT(1) FROM related_product WHERE product_id = :productId) WHERE product_id = :productId LIMIT 1;";

        $this->_db->createCommand( $updateMasterRelatedProductCountSQL )
                  ->execute(
                      array(
                          ':productId' => $masterProductId
                      )
                  );
    }

    /**
     * This function updates Master products so their ecommerce products are the same as their duplicates' categories
     *
     * @param int   $masterProductId
     * @param int   $masterRootCategoryId
     * @param array $subordinateProductIdsArray
     */
    private function updateMasterProductEcommerceProducts( $masterProductId, $masterRootCategoryId, array $subordinateProductIdsArray )
    {
        $this->debugMessage( "Updating ecommerce products" );

        $subordinateProductIdBindings = Utilities::createQueryBindings( $subordinateProductIdsArray, 'subPrId' );

        // if we can find any subordinate ecommerce links
        if( $subordinateEcommerceProducts = $this->_db->createCommand(
            "SELECT * FROM ecommerce_provider_product WHERE product_id IN (" . implode( ',', array_keys( $subordinateProductIdBindings ) ) . ");"
        )
                                                      ->bindValues( $subordinateProductIdBindings )
                                                      ->queryAll()
        )
        {
            // one month old
            $oldLinkAge = time() - 2592000;

            // work out what we want the master's ecommerce products to be
            $desiredEcommerceProducts = [];
            foreach( $subordinateEcommerceProducts as $ecommerceProduct )
            {
                // if we don't yet have a product with this provider, country and name or if this one has been updated more recently ten the one we have
                if( empty( $desiredEcommerceProducts[$ecommerceProduct['ecommerce_provider_id']][$ecommerceProduct['country_code']][$ecommerceProduct['product_name']] )
                    || $desiredEcommerceProducts[$ecommerceProduct['ecommerce_provider_id']][$ecommerceProduct['country_code']][$ecommerceProduct['product_name']]['last_updated']
                       < $ecommerceProduct['last_updated']
                )
                {
                    // update the desired products
                    $desiredEcommerceProducts[$ecommerceProduct['ecommerce_provider_id']][$ecommerceProduct['country_code']][$ecommerceProduct['product_name']] = $ecommerceProduct;
                    $desiredEcommerceProducts[$ecommerceProduct['ecommerce_provider_id']][$ecommerceProduct['country_code']][$ecommerceProduct['product_name']]['product_id'] = $masterProductId;
                }

                // if the ecommerce link is old then queue it for updating
                if( $ecommerceProduct['last_updated'] < $oldLinkAge )
                {
                    switch( $ecommerceProduct['ecommerce_provider_id'] )
                    {
                        case AmazonDataProvider::create()
                                               ->getEcommerceProviderId():
                            AmazonDataProvider::create()
                                              ->getEcommerceLinkByProductIdByCountryQueued(
                                                  array(
                                                      'product_id'   => $ecommerceProduct['product_id'],
                                                      'country_code' => $ecommerceProduct['country_code']
                                                  ),
                                                  true
                                              );
                    }
                }
            }

            // get the current ecommerce products
            $currentEcommerceProducts = $this->_db->createCommand( "SELECT * FROM ecommerce_provider_product WHERE product_id = :mstPrId" )
                                                  ->bindValue( ':mstPrId', $masterProductId )
                                                  ->queryAll();

            $excessEcommerceProducts = [];

            foreach( $currentEcommerceProducts as $currentEcommerceProduct )
            {
                // if we want a product of this type
                if( isset( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']][$currentEcommerceProduct['country_code']][$currentEcommerceProduct['product_name']] ) )
                {
                    // if it's already up to date
                    if( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']][$currentEcommerceProduct['country_code']][$currentEcommerceProduct['product_name']]
                        == $currentEcommerceProduct
                    )
                    {
                        // it doesn't need updating
                        unset( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']][$currentEcommerceProduct['country_code']][$currentEcommerceProduct['product_name']] );

                        // if none of the country's products needed updating
                        if( empty( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']][$currentEcommerceProduct['country_code']] ) )
                        {
                            // remove the country
                            unset( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']][$currentEcommerceProduct['country_code']] );
                        }

                        // if none of the provider's products needed updating
                        if( empty( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']] ) )
                        {
                            // remove the provider
                            unset( $desiredEcommerceProducts[$currentEcommerceProduct['ecommerce_provider_id']] );
                        }
                    }
                }
                else
                {
                    // list it for deleting
                    $excessEcommerceProducts[] = $currentEcommerceProduct;
                }
            }

            //if any ecommerce products need deleting
            if( $excessEcommerceProducts )
            {
                $this->debugMessage( ['excess ecommerce products' => $excessEcommerceProducts], 2 );

                // build PDO query & bindings
                $deletionBindingLocations = [];
                $deletionBindings = [];
                $deletionCounter = 0;
                foreach( $excessEcommerceProducts as $excessEcommerceProduct )
                {
                    $deletionBindings[( $deletionProviderBinding = ":prvdr" . ++$deletionCounter )] = $excessEcommerceProduct['ecommerce_provider_id'];
                    $deletionBindings[( $deletionCountryBinding = ":cntr{$deletionCounter}" )] = $excessEcommerceProduct['country_code'];
                    $deletionBindings[( $deletionNameBinding = ":nm{$deletionCounter}" )] = $excessEcommerceProduct['product_name'];

                    $deletionBindingLocations[] = "(ecommerce_provider_id = {$deletionProviderBinding} AND country_code = {$deletionCountryBinding} AND product_name = {$deletionNameBinding})";
                }

                // delete excess products
                $this->_db->createCommand(
                    "DELETE FROM ecommerce_provider_product WHERE product_id = :mstPrId AND ( " . implode( " OR ", $deletionBindingLocations ) . " );"
                )
                          ->bindValue( ':mstPrId', $masterProductId )
                          ->bindValues( $deletionBindings )
                          ->execute();
            }

            //if any ecommerce products need creating or updating
            if( $desiredEcommerceProducts )
            {
                $this->debugMessage( ['missing or out of date ecommerce products' => $desiredEcommerceProducts], 2 );

                // build PDO query & bindings
                $creationBindingLocations = [];
                $creationBindings = [];
                $creationCounter = 0;
                foreach( $desiredEcommerceProducts as $desiredDataProviderEcommerceProducts )
                {
                    foreach( $desiredDataProviderEcommerceProducts as $desiredCountryEcommerceProducts )
                    {
                        foreach( $desiredCountryEcommerceProducts as $desiredEcommerceProduct )
                        {
                            $creationBindings[( $creationProviderBinding = ":prvdr" . ++$creationCounter )] = $desiredEcommerceProduct['ecommerce_provider_id'];
                            $creationBindings[( $creationCountryBinding = ":cntr{$creationCounter}" )] = $desiredEcommerceProduct['country_code'];
                            $creationBindings[( $creationNameBinding = ":nm{$creationCounter}" )] = $desiredEcommerceProduct['product_name'];
                            $creationBindings[( $creationLinkBinding = ":lnk{$creationCounter}" )] = $desiredEcommerceProduct['ecommerce_link'];
                            $creationBindings[( $creationImageBinding = ":img{$creationCounter}" )] = $desiredEcommerceProduct['image_url'];
                            $creationBindings[( $creationPriceBinding = ":prc{$creationCounter}" )] = $desiredEcommerceProduct['price'];
                            $creationBindings[( $creationCurrencyBinding = ":crnc{$creationCounter}" )] = $desiredEcommerceProduct['currency_code'];
                            $creationBindings[( $creationUpdatedBinding = ":upd{$creationCounter}" )] = $desiredEcommerceProduct['last_updated'];
                            $creationBindingLocations[] = "({$creationProviderBinding},:mstPrId,{$creationCountryBinding},{$creationNameBinding},{$creationLinkBinding},{$creationImageBinding},{$creationPriceBinding},{$creationCurrencyBinding},{$creationUpdatedBinding})";
                        }
                    }
                }

                // insert or update desired products
                $this->_db->createCommand(
                    "
INSERT INTO
  ecommerce_provider_product
  (ecommerce_provider_id, product_id, country_code, product_name, ecommerce_link, image_url, price, currency_code, last_updated)
VALUES
" . implode( ',', $creationBindingLocations ) . "
ON DUPLICATE KEY UPDATE
 ecommerce_link = VALUES(ecommerce_link),
 image_url = VALUES(image_url),
 price = VALUES(price),
 currency_code = VALUES(currency_code),
 last_updated = VALUES(last_updated)"
                )
                          ->bindValue( ':mstPrId', $masterProductId )
                          ->bindValues( $creationBindings )
                          ->execute();
            }
        }
        else
        {
            // if we have no subordinate ecommerce links then create some (force cache ignore)
            $this->_productService->createProductEcommerceLinks( array('product_id' => $masterProductId, 'root_category_id' => $masterRootCategoryId), true );
        }
    }

    /**
     * This function updates a master product's ratings so that any that are missing or more recent among its subordinates are used instead
     *
     * @param int   $masterProductId
     * @param int   $masterRootCategoryId
     * @param array $subordinateProductIdsArray
     */
    private function updateMasterProductRatings( $masterProductId, $masterRootCategoryId, array $subordinateProductIdsArray )
    {
        $this->debugMessage( "Updating ratings" );
        // get the current user ratings of the master
        $masterRatingsSQL = "SELECT user_id, last_updated FROM user_rating WHERE product_id = :productId AND root_category_id = :rootCategoryId;";

        $masterRatingsResult = $this->_db->createCommand( $masterRatingsSQL )
                                         ->queryAll(
                                             true,
                                             array(
                                                 ':productId'      => $masterProductId,
                                                 ':rootCategoryId' => $masterRootCategoryId
                                             )
                                         );

        $masterRatingUserIdsArray = array_column( $masterRatingsResult, 'user_id' );

        $masterRatingLastUpdatedArray = array_column( $masterRatingsResult, 'last_updated', 'user_id' );

        $this->debugMessage( "Current rating user ids : " . implode( ', ', $masterRatingUserIdsArray ), 2 );

        // get the most resent ratings among the subordinates
        $subordinateRatingsSQL = "SELECT user_id, last_updated, rating FROM user_rating WHERE product_id IN (" . implode( ',', $subordinateProductIdsArray ) . ")
                                    AND root_category_id = :rootCategoryId ORDER BY user_id, last_updated ASC;";

        $subordinateRatingsResult = $this->_db->createCommand( $subordinateRatingsSQL )
                                              ->queryAll(
                                                  true,
                                                  array(
                                                      ':rootCategoryId' => $masterRootCategoryId
                                                  )
                                              );

        // extract a simple list of user ids of ratings
        $subordinateRatingUserIdsArray = array_column( $subordinateRatingsResult, 'user_id', 'user_id' );
        $this->debugMessage( "desired rating user ids : " . implode( ', ', $subordinateRatingUserIdsArray ), 2 );

        // get an associative array of last_updated of user ratings
        $subordinateRatingLastUpdatedArray = array_column( $subordinateRatingsResult, 'last_updated', 'user_id' );
        $subordinateRatingRatingsArray = array_column( $subordinateRatingsResult, 'rating', 'user_id' );
        $this->debugMessage( "ratings last updated : " . json_encode( $subordinateRatingLastUpdatedArray ), 3 );

        // unlike related products/categories/images we don't want to remove excess ratings as it's possible to rate the master product directly

        // get a list of products that need to be added
        $missingRatingUserIdsArray = array_diff( $subordinateRatingUserIdsArray, $masterRatingUserIdsArray );

        if( !empty( $missingRatingUserIdsArray ) )
        {
            $this->debugMessage( "Adding missing user ratings", 2 );
            $this->debugMessage( implode( ', ', $missingRatingUserIdsArray ), 3 );
            /**
             * @var UserRating[] $newUserRatingsArray
             */
            $newUserRatingsArray = array();

            // add each missing rating
            foreach( $missingRatingUserIdsArray as $missingRatingUserId )
            {
                $newUserRatingsArray[$missingRatingUserId] = new UserRating();
                $newUserRatingsArray[$missingRatingUserId]->setAttributes(
                    array(
                        'user_id'          => $missingRatingUserId,
                        'product_id'       => $masterProductId,
                        'rating'           => $subordinateRatingRatingsArray[$missingRatingUserId],
                        'last_updated'     => $subordinateRatingLastUpdatedArray[$missingRatingUserId],
                        'root_category_id' => $masterRootCategoryId
                    )
                );
                $newUserRatingsArray[$missingRatingUserId]->save();
            }
        }

        // get a list of ratings that need to be updated because there is a subordinate that has had its rating updated more recently than the master's
        $outOfDateRatingLastUpdatedArray = array_udiff_assoc(
            array_intersect_key( $subordinateRatingLastUpdatedArray, $masterRatingLastUpdatedArray ),
            $masterRatingLastUpdatedArray,
            function ( $a, $b )
            {
                return $a > $b;
            }
        );

        if( !empty( $outOfDateRatingLastUpdatedArray ) )
        {
            $this->debugMessage( "Updating out of date user ratings", 2 );
            $this->debugMessage( implode( ', ', array_keys( $outOfDateRatingLastUpdatedArray ) ), 3 );
            /**
             * @var UserRating[] $updatedUserRatingArray
             */
            $updatedUserRatingArray = array();
            foreach( $outOfDateRatingLastUpdatedArray as $outOfDateRatingUserId => $outOfDateRatingNewLastUpdated )
            {
                // find each out of date related product
                if( $updatedUserRatingArray[$outOfDateRatingUserId] = UserRating::model()
                                                                                ->findByAttributes(
                                                                                    array(
                                                                                        'product_id' => $masterProductId,
                                                                                        'user_id'    => $outOfDateRatingUserId,
                                                                                    )
                                                                                )
                )
                {
                    // update it
                    $updatedUserRatingArray[$outOfDateRatingUserId]->setAttributes(
                        array(
                            'last_updated' => $outOfDateRatingNewLastUpdated,
                            'rating'       => $subordinateRatingRatingsArray[$outOfDateRatingUserId]
                        )
                    );
                    $updatedUserRatingArray[$outOfDateRatingUserId]->save();
                }
            }
        }
    }

    /**
     * This function updates a master product's reviews so that any that are missing or more recent among its subordinates are used instead.
     *
     * @todo: comments, likes, comment likes
     *
     * @param int   $masterProductId
     * @param int   $masterRootCategoryId
     * @param array $subordinateProductIdsArray
     */
    private function updateMasterProductReviews( $masterProductId, $masterRootCategoryId, array $subordinateProductIdsArray )
    {
        $this->debugMessage( "Updating reviews" );
        // get the current user reviews of the master
        $masterReviewsSQL = "SELECT user_id, last_updated FROM user_review WHERE product_id = :productId;";

        $masterReviewsResult = $this->_db->createCommand( $masterReviewsSQL )
                                         ->queryAll(
                                             true,
                                             array(
                                                 ':productId' => $masterProductId
                                             )
                                         );

        $masterReviewUserIdsArray = array_column( $masterReviewsResult, 'user_id' );

        $masterReviewLastUpdatedArray = array_column( $masterReviewsResult, 'last_updated', 'user_id' );

        $this->debugMessage( "Current review user ids : " . implode( ', ', $masterReviewUserIdsArray ), 2 );

        // get the most resent reviews among the subordinates
        $subordinateReviewsSQL = "SELECT ure.user_id, ure.last_updated, ure.review_note, ure.review_date, ure.review_link
                                  FROM user_review ure JOIN user_rating ura
                                  ON ure.user_id = ura.user_id AND ure.product_id = ura.product_id
                                  WHERE ure.product_id IN (" . implode( ',', $subordinateProductIdsArray ) . ")
                                  AND ura.root_category_id = :rootCategoryId
                                  ORDER BY ure.user_id, ure.last_updated ASC;";

        $subordinateReviewsResult = $this->_db->createCommand( $subordinateReviewsSQL )
                                              ->queryAll(
                                                  true,
                                                  array(
                                                      ':rootCategoryId' => $masterRootCategoryId
                                                  )
                                              );

        // extract a simple list of user ids of reviews
        $subordinateReviewUserIdsArray = array_column( $subordinateReviewsResult, 'user_id', 'user_id' );
        $this->debugMessage( "desired review user ids : " . implode( ', ', $subordinateReviewUserIdsArray ), 2 );

        // get an associative array of last_updated of user reviews
        $subordinateReviewLastUpdatedArray = array_column( $subordinateReviewsResult, 'last_updated', 'user_id' );
        $subordinateReviewReviewDatesArray = array_column( $subordinateReviewsResult, 'review_date', 'user_id' );
        $subordinateReviewReviewNotesArray = array_column( $subordinateReviewsResult, 'review_note', 'user_id' );
        $subordinateReviewReviewLinksArray = array_column( $subordinateReviewsResult, 'review_link', 'user_id' );
        $this->debugMessage( "reviews last updated : " . json_encode( $subordinateReviewLastUpdatedArray ), 3 );

        // unlike related products/categories/images we don't want to remove excess reviews as it's possible to rate the master product directly

        // get a list of products that need to be added
        $missingReviewUserIdsArray = array_diff( $subordinateReviewUserIdsArray, $masterReviewUserIdsArray );

        if( !empty( $missingReviewUserIdsArray ) )
        {
            $this->debugMessage( "Trying to add missing user reviews", 2 );
            $this->debugMessage( implode( ', ', $missingReviewUserIdsArray ), 3 );
            /**
             * @var UserReview[] $newUserReviewsArray
             */
            // $newUserReviewsArray = array();

            foreach( $missingReviewUserIdsArray as $missingReviewUserId )
            {
                // if the product has a rating by the right user
                if( UserRating::model()
                              ->findByAttributes(
                                  array(
                                      'user_id'    => $missingReviewUserId,
                                      'product_id' => $masterProductId
                                  )
                              )
                )
                {
                    // make a new review

                    /*
                    $missingReviewInsertSQL = "INSERT IGNORE INTO user_review (user_id, product_id, review_date, review_note, review_link, last_updated)
                        VALUES($missingReviewUserId, $masterProductId, '$subordinateReviewReviewDatesArray[$missingReviewUserId]', '$subordinateReviewReviewNotesArray[$missingReviewUserId]',
                        '$subordinateReviewReviewLinksArray[$missingReviewUserId]', '$subordinateReviewLastUpdatedArray[$missingReviewUserId]')";
                    YiiItcher::app()->db->createCommand( $missingReviewInsertSQL )
                                        ->execute();
                    */
                    $newUserReviewsArray[$missingReviewUserId] = new UserReview();
                    $newUserReviewsArray[$missingReviewUserId]->setAttributes(
                        array(
                            'user_id'      => $missingReviewUserId,
                            'product_id'   => $masterProductId,
                            'review_date'  => $subordinateReviewReviewDatesArray[$missingReviewUserId],
                            'review_note'  => $subordinateReviewReviewNotesArray[$missingReviewUserId],
                            'review_link'  => $subordinateReviewReviewLinksArray[$missingReviewUserId],
                            'last_updated' => $subordinateReviewLastUpdatedArray[$missingReviewUserId],
                        )
                    );
                    $newUserReviewsArray[$missingReviewUserId]->save();
                }
                else
                {
                    // don't make a new review
                    $this->debugMessage( "missing rating user id : $missingReviewUserId", 2 );
                }
            }
        }

        // get a list of reviews that need to be updated because there is a subordinate that has had its review updated more recently than the master's
        $outOfDateReviewLastUpdatedArray = array_udiff_assoc(
            array_intersect_key( $subordinateReviewLastUpdatedArray, $masterReviewLastUpdatedArray ),
            $masterReviewLastUpdatedArray,
            function ( $a, $b )
            {
                return $a > $b;
            }
        );

        if( !empty( $outOfDateReviewLastUpdatedArray ) )
        {
            $this->debugMessage( "Updating out of date user reviews", 2 );
            $this->debugMessage( implode( ', ', array_keys( $outOfDateReviewLastUpdatedArray ) ), 3 );
            /**
             * @var UserReview[] $updatedUserReviewArray
             */
            $updatedUserReviewArray = array();
            foreach( $outOfDateReviewLastUpdatedArray as $outOfDateReviewUserId => $outOfDateReviewNewLastUpdated )
            {
                // find each out of date related product
                if( $updatedUserReviewArray[$outOfDateReviewUserId] = UserReview::model()
                                                                                ->findByAttributes(
                                                                                    array(
                                                                                        'product_id' => $masterProductId,
                                                                                        'user_id'    => $outOfDateReviewUserId,
                                                                                    )
                                                                                )
                )
                {
                    // update it
                    $updatedUserReviewArray[$outOfDateReviewUserId]->setAttributes(
                        array(
                            'last_updated' => $outOfDateReviewNewLastUpdated,
                            'review_date'  => $subordinateReviewReviewDatesArray[$outOfDateReviewUserId],
                            'review_note'  => $subordinateReviewReviewNotesArray[$outOfDateReviewUserId],
                            'review_link'  => $subordinateReviewReviewLinksArray[$outOfDateReviewUserId],
                        )
                    );
                    $updatedUserReviewArray[$outOfDateReviewUserId]->save();
                }
            }
        }
    }

    /**
     * This function updates a product in the database (it won't create a new product)
     *
     * @param int   $assemblerProductId
     * @param array $assembledProduct
     *
     * @return bool
     */
    public function saveAssembledProduct( $assemblerProductId, array $assembledProduct )
    {
        $this->debugMessage( "preparing to save product : $assemblerProductId", 2 );

        // construct the various derived attributes saved in the product table
        $assembledProduct['product_sort_name'] = ( preg_match( "/^The /", $assembledProduct['product_name'] ) ?
            substr( $assembledProduct['product_name'], 4 ) . ", The" :
            ( preg_match( "/^A /", $assembledProduct['product_name'] ) ?
                substr( $assembledProduct['product_name'], 2 ) . ", A" :
                $assembledProduct['product_name'] ) );
        $assembledProduct['product_name_md5'] = md5( strtolower( $assembledProduct['product_name'] ), true );
        $assembledProduct['product_name_soundex_lookup'] = $this->createProductNameSoundex( $assembledProduct['product_name'] );

        // if the product has a description (that isn't an empty string or the like)
        if( !empty( $assembledProduct['product_description'] ) )
        {
            if( $assembledProduct['product_root_category_id'] == self::musicRootId() && !$this->isSubproduct( $assemblerProductId ) && empty( $assembledProduct['product_attributes']['album_id'] ) && empty( $assembledProduct['product_attributes']['single_id'] ) )
            {
                // if it's an artist with no releases don't display it
                $assembledProduct['display'] = 0;
            }
            else
            {
                // set the product to be displayed (it could still be archived and if so will be over written later)
                $assembledProduct['display'] = 1;
            }
        }
        else
        {
            // else set the product to not display and set the description as 'No description yet'
            $assembledProduct['display'] = 0;
            $assembledProduct['product_description'] = self::$_NO_DESCRIPTION_YET;
        }

        $updated = false;

        /**
         * @var Product $product
         */
        // get the product object for this product
        $product = Product::model()
                          ->findByPk( $assemblerProductId );

        // update the product objects properties
        foreach( $assembledProduct as $attributeName => $attributeValue )
        {
            if( array_key_exists( $attributeName, $product->getAttributes() ) && $product->$attributeName != $attributeValue )
            {
                $product->setAttribute( $attributeName, $attributeValue );
                $updated = true;
            }
        }

        // save the ProductAttributes separately
        $updated = $this->saveProductAttributes( $assemblerProductId, $assembledProduct['product_attributes'] ) || $updated;

        // if the product is archived or has no images set it not to display
        if( $product->display
            && ( $product->archived == 1 or !( $this->_db->createCommand( "SELECT TRUE FROM product_image WHERE download_error !=1 AND product_id = :productId;" )
                                                         ->queryScalar( array(':productId' => $assemblerProductId) ) ) )
        )
        {
            $product->display = 0;
            $updated = true;
        }

        // If it's the product for 'various' artists
        if( $product->product_id == $this->variousArtistsProductId )
        {
            // set archived and display to 0
            $product->display = $product->archived = 0;
        }

        if( $updated )
        {
            $product->last_updated = time();
            // save the product
            $product->save();
            $this->debugMessage( "saved product : $assemblerProductId\n" );
        }
        else
        {
            $this->debugMessage( "No changes in product : $assemblerProductId" );
        }

        return $updated;
    }

    /**
     * This function saves a product's attributes, updating existing attributes where possible.
     * It deletes excess attributes and creates new ones where necessary.
     *
     * @param int   $productId
     * @param array $productAttributesArray
     *
     * @return boolean
     *
     * @throws Exception
     */
    private function saveProductAttributes( $productId, array $productAttributesArray )
    {
        /**
         * @var ProductAttribute[]   $unsortedCurrentAttributesArray
         * @var ProductAttribute[][] $currentProductAttributesByTypeArray
         */
        // get existing attributes from the db
        $unsortedCurrentAttributesArray = ProductAttribute::model()
                                                          ->findAllByAttributes(
                                                              array(
                                                                  'product_id' => $productId
                                                              )
                                                          );
        /** @var ProductAttribute[][] $currentProductAttributesByTypeArray */
        $currentProductAttributesByTypeArray = array();
        foreach( $unsortedCurrentAttributesArray as $unsortedCurrentAttribute )
        {
            // group the existing attributes by attribute type id
            $currentProductAttributesByTypeArray[$unsortedCurrentAttribute->product_attribute_type_id][] = $unsortedCurrentAttribute;
        }
        $productAttributesByTypeArray = array();
        foreach( $productAttributesArray as $productAttributeTypeName => $productAttributeValue )
        {
            // group the desired attributes by type id
            $productAttributeTypeId = ProductAttributeType::getIdFromName( $productAttributeTypeName );

            /*
             * Had to turn of E_NOTICE for this line to suppress integer overflow warnings
             */
            $reportMode = error_reporting(); // save current error reporting
            error_reporting( $reportMode & ~E_NOTICE ); // turn off E_NOTICE level errors
            // try to json_decode the attribute
            $productAttributeValueDecoded = json_decode( $productAttributeValue, true, 512, JSON_BIGINT_AS_STRING );
            error_reporting( $reportMode );// go back to old error reporting

            // if we need to load the product attribute type ids for the attributes we wish to load then do so
            if( !isset( $this->_attributeTypeIds['author'] ) )
            {
                $this->_attributeTypeIds['author'] = ProductAttributeType::getIdFromName( 'auhor' );
            }
            if( !isset( $this->_attributeTypeIds['developer'] ) )
            {
                $this->_attributeTypeIds['developer'] = ProductAttributeType::getIdFromName( 'developer' );
            }
            if( !isset( $this->_attributeTypeIds['genre'] ) )
            {
                $this->_attributeTypeIds['genre'] = ProductAttributeType::getIdFromName( 'genre' );
            }
            if( !isset( $this->_attributeTypeIds['platform'] ) )
            {
                $this->_attributeTypeIds['platform'] = ProductAttributeType::getIdFromName( 'platform' );
            }
            if( !isset( $this->_attributeTypeIds['related_user'] ) )
            {
                $this->_attributeTypeIds['related_user'] = ProductAttributeType::getIdFromName( 'related_user' );
            }
            if( !isset( $this->_attributeTypeIds['artist'] ) )
            {
                $this->_attributeTypeIds['artist'] = ProductAttributeType::getIdFromName( 'artist' );
            }

            // if the decoding worked and produced an ordered array
            if( $productAttributeValueDecoded && is_array( $productAttributeValueDecoded )
                && array_keys( $productAttributeValueDecoded ) === range(
                    0,
                    count(
                        $productAttributeValueDecoded
                    ) - 1
                )
            )
            {
                switch( $productAttributeTypeId )
                {
                    case $this->_attributeTypeIds['author']:
                    case $this->_attributeTypeIds['developer']:
                    case $this->_attributeTypeIds['genre']:
                    case $this->_attributeTypeIds['platform']:
                    case $this->_attributeTypeIds['related_user']:
                    case $this->_attributeTypeIds['artist']:
                        // if the attribute type is one we want to run searches against and it's an indexed array
                        foreach( $productAttributeValueDecoded as $productAttributeValueDecodedSingle )
                        {
                            // store each of the values separately
                            if( is_array( $productAttributeValueDecodedSingle ) )
                            {
                                $productAttributesByTypeArray[$productAttributeTypeId][] = json_encode( $productAttributeValueDecodedSingle );
                            }
                            else
                            {
                                $productAttributesByTypeArray[$productAttributeTypeId][] = $productAttributeValueDecodedSingle;
                            }
                        }
                        break;
                    default:
                        // if it's a sequential indexed array but we don't want to run searches against it store it as a single json
                        $productAttributesByTypeArray[$productAttributeTypeId][0] = $productAttributeValue;
                        break;
                }
            }
            else
            {
                // if it isn't a sequential indexed array store it as is (either a json or a normal string)
                $productAttributesByTypeArray[$productAttributeTypeId][0] = $productAttributeValue;
            }
        }

        $updatedAttributes = false;

        // get an array of attribute types that we have and want
        foreach( $productAttributesByTypeArray as $attributeTypeId => $productAttributeByType )
        {
            if( isset( $currentProductAttributesByTypeArray[$attributeTypeId] ) )
            {
                foreach( $currentProductAttributesByTypeArray[$attributeTypeId] as $currentKey => $currentProductAttribute )
                {
                    if( ( $key = array_search( $currentProductAttribute->product_attribute_value, $productAttributeByType ) ) !== false )
                    {
                        unset( $productAttributeByType[$key], $currentProductAttributesByTypeArray[$attributeTypeId][$currentKey] );
                    }
                }

                foreach( $productAttributeByType as $key => $value )
                {
                    if( $currentProductAttribute = array_pop( $currentProductAttributesByTypeArray[$attributeTypeId] ) )
                    {
                        $currentProductAttribute->setAttribute( 'product_attribute_value', $value );
                        $currentProductAttribute->save();
                        $updatedAttributes = true;
                    }
                    else
                    {
                        $currentProductAttribute = new ProductAttribute();
                        $currentProductAttribute->setAttributes(
                            array(
                                'product_id'                => $productId,
                                'product_attribute_type_id' => $attributeTypeId,
                                'product_attribute_value'   => $value,
                            )
                        );
                        $currentProductAttribute->save();
                        $updatedAttributes = true;
                    }
                }

                foreach( $currentProductAttributesByTypeArray[$attributeTypeId] as $currentKey => $currentProductAttribute )
                {
                    // delete each ProductAttributes
                    $currentProductAttribute->delete();
                    unset( $currentProductAttributesByTypeArray[$attributeTypeId][$currentKey] );
                    $updatedAttributes = true;
                }
            }
            else
            {
                foreach( $productAttributeByType as $key => $value )
                {
                    // create a ProductAttribute for each attribute value, set its attributes and save it
                    $currentProductAttribute = new ProductAttribute();
                    $currentProductAttribute->setAttributes(
                        array(
                            'product_id'                => $productId,
                            'product_attribute_type_id' => $attributeTypeId,
                            'product_attribute_value'   => $value,
                        )
                    );
                    $currentProductAttribute->save();
                    $updatedAttributes = true;
                }
            }
        }

        return $updatedAttributes;
    }

    /**
     * This function checks if a product needs updating.
     * It's assumed to need updating if any of its data provider products or subordinate products have been updated more recently than it has
     *
     * @param $validDataArray
     * @param $productId
     *
     * @return bool
     */
    public function checkNeedsUpdating( $validDataArray, $productId )
    {
        // Get the time (unix epoch) the product was last updated
        $productLastUpdated = 0;
        foreach( $validDataArray['product'] as $product )
        {
            if( $product['product_id'] == $productId )
            {
                $productLastUpdated = $product['last_updated'];
                break;
            }
        }

        // If it's a new product it needs updating
        if( $productLastUpdated == 0 )
        {
            return true;
        }

        // if any of its data provider products have been updated more recently than it it needs updating
        foreach( $validDataArray['data_provider_product'] as $dataProviderProduct )
        {
            if( $dataProviderProduct['last_updated'] > $productLastUpdated )
            {
                return true;
            }
        }

        // if any of its component products have been updated then it it needs updating
        if( count( $validDataArray['product'] ) > 1 )
        {
            foreach( $validDataArray['product'] as $product )
            {
                if( $product['product_id'] != $productId && $product['last_updated'] > $productLastUpdated )
                {
                    return true;
                }
            }
        }

        // if none of those are the case it doesn't need updating
        return false;
    }

    /**
     * this function performs a quick sanity check on products marked as a duplicate before using data from them in this one.
     *
     * @param int   $assemblerProductId
     * @param array $productsArray
     * @param array $productAttributesArray
     * @param array $dataProviderProductsArray
     *
     * @return array
     */
    public function defaultDataValidation( $assemblerProductId, array $productsArray, array $productAttributesArray, array $dataProviderProductsArray )
    {
        $validProductIds = array($assemblerProductId);

        $this->debugMessage( "Valid product ids : " . implode( ',', $validProductIds ) );
        $validDataArray = array(
            'products_array'               => $productsArray,
            'product_attributes_array'     => $productAttributesArray,
            'data_provider_products_array' => $dataProviderProductsArray,
        );

        return $validDataArray;

    }
}