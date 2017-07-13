<?php

/**
 */
class ProductDuplicationService extends AbstractDataProvider
{
    public $debug = false;

    public $cache;

    const MASTER_SUBORDINATE_DUPLICATE_RELATION_ID = 1;
    const MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID = 2;
    const CONFLICTED_RELATION_ID = 3;
    const MARKED_AS_UNIQUE_RELATION_ID = 4;
    const MASTER_MASTER_RELATION_ID = 5;
    const CONFLICTED_MASTER_RELATION_ID = 6;
    const QUERY_QUEUE_NAME = 'y_msg_queue_duplication_queue';
    const INSERT_QUEUE_NAME = 'y_msg_queue_duplication_migration';
    const PRODUCT_RELATIONS_CACHE_NAME = 'product_relations_for_product_';

    const UNIQUE_PRODUCT_NAME = 'Unique';

    const MASTER_PRODUCT_NAME = 'Master';

    const MASTER_SUBORDINATE_PRODUCT_IDS_KEY = 'subordinate_product_ids';

    const MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY = 'duplicate_master_product_ids';

    const SUBORDINATE_PRODUCT_NAME = 'Subordinate';

    const SUBORDINATE_MASTER_PRODUCT_ID_KEY = 'master_product_id';

    const CONFLICTED_PRODUCT_NAME = 'Conflicted';

    const CONFLICTED_MASTER_PRODUCT_IDS_KEY = 'conflicting_master_product_ids';

    const RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP = 'resolved_as_not_duplicate';
    const RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY = 'resolved_as_not_duplicate_product_ids';

    const BROKEN_RELATIONSHIP = 'broken';

    const CACHE_STRING_FALSE = 'FALSE';
    const CACHE_STRING_TRUE = 'TRUE';
    const CACHE_TIME_ONE_WEEK = 604800;

    /**unused**/
    const SUBORDINATE_MASTER_RELATIONSHIP = 'subordinate_master';
    const MASTER_MASTER_RELATIONSHIP = 'master_master';

    /**unused**/

    private $_newName = 'Unique';

    /**
     * @var ProductAssemblerService
     */
    private $_productAssemblerService;

    private $_attributeTypeIds = array();

    // private $_subordinateProductSubproductTypeId;

    // const SUBORDINATE_PRODUCT_SUBPRODUCT_TYPE_NAME = 'subordinate_product';

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

    private $_editionNameIndicators = array(
        'edition',
        'limited',
        'collector',
        'annotated',
        'classics',
    );

    private $_nameStopWords = array(
        'a',
        'the',
    );

    private $_romanNumerals = array(
        'ii'    => 2,
        'iii'   => 3,
        'iv'    => 4,
        'v'     => 5,
        'vi'    => 6,
        'vii'   => 7,
        'viii'  => 8,
        'ix'    => 9,
        'x'     => 10,
        'xi'    => 11,
        'xii'   => 12,
        'xiii'  => 13,
        'xiv'   => 14,
        'xv'    => 15,
        'xvi'   => 16,
        'xvii'  => 17,
        'xviii' => 18,
        'xix'   => 19,
        'xx'    => 20,
    );

    private $_englishNumerals = array(
        'zero'      => 0,
        'one'       => 1,
        'two'       => 2,
        'three'     => 3,
        'four'      => 4,
        'five'      => 5,
        'six'       => 6,
        'seven'     => 7,
        'eight'     => 8,
        'nine'      => 9,
        'ten'       => 10,
        'eleven'    => 11,
        'twelve'    => 12,
        'thirteen'  => 13,
        'fourteen'  => 14,
        'fifteen'   => 15,
        'sixteen'   => 16,
        'seventeen' => 17,
        'eighteen'  => 18,
        'nineteen'  => 19,
        'twenty'    => 20,
    );

    private $_thresholdPercentScore = 0.75;

    private $_minimumPercentScore = 0.25;

    private $productId = null;

    private $lastUpdated = null;

    private $relations = array();

    /**
     * @param string                  $countryCode
     * @param ProductAssemblerService $assemblerService
     *
     * @throws CException
     */
    public function init( $countryCode, ProductAssemblerService $assemblerService = null )
    {
        parent::init( $countryCode );

        if( isset( $assemblerService ) && is_a( $assemblerService, 'ProductAssemblerService' ) )
        {
            $this->_productAssemblerService = $assemblerService;
        }
        else
        {
            $this->_productAssemblerService = new ProductAssemblerService( $this );
        }

        if( $this->dataProviderId == null )
        {
            $this->dataProviderId = DataProvider::model()
                                                ->getDataProviderFromName( ProductAssemblerService::ADMIN_USER_DATA_PROVIDER_NAME )
                                                ->getPrimaryKey();
        }
        // for each attribute
        foreach( $this->_attributeScoresArray['attribute_match'] as $attributeTypeName => $matchScore )
        {
            // get its type id
            $attributeTypeId = ProductAttributeType::getIdFromName( $attributeTypeName );

            $this->debugMsg( $attributeTypeName . '::' . $attributeTypeId, 1 );

            // add it to the array of type ids
            $this->_attributeTypeIds[$attributeTypeName] = $attributeTypeId;

            // add its matching score to the array by type id
            $this->_duplicateScores['attribute_match'][$attributeTypeId] = $matchScore;

            // add its mismatching score to the array by type id
            $this->_duplicateScores['attribute_mismatch'][$attributeTypeId] = $this->_attributeScoresArray['attribute_mismatch'][$attributeTypeName];
        }
        if( isset( $this->debug ) )
        {
            $this->_productAssemblerService->setDebug( $this->debug, false );
        }
    }

    public function setDebug( $enabled = true, $recursive = true )
    {
        parent::setDebug( $enabled );
        if( $recursive && isset( $this->_productAssemblerService ) )
        {
            $this->_productAssemblerService->setDebug( $enabled, false );
        }
    }

    /**
     * @param int  $productId
     * @param bool $queueOnly
     * @param bool $forceUpdate
     * @param bool $queueAssembler
     * @param bool $queueDownstream
     */
    public function updateProduct( $productId, $queueOnly = false, $forceUpdate = false, $queueAssembler = true, $queueDownstream = true )
    {
        $params = array(
            'product_id' => $productId,
        );
        if( $forceUpdate )
        {
            $params['force_update'] = $forceUpdate;
        }

        return $this->updateProductQueued( $params, $queueOnly, $queueAssembler, $queueDownstream );
    }

    /**
     * this function updates a product's productDuplicationDataProviderProduct, and possibly those of other products detected as duplicates of this one
     *
     * @param array $params
     * @param bool  $queueOnly
     * @param bool  $queueAssembler
     * @param bool  $queueDownstream
     *
     * @todo test
     * @return null
     */
    private function updateProductQueued( array $params, $queueOnly = false, $queueAssembler = true, $queueDownstream = true )
    {
        if( $queueOnly )
        {
            $this->queueDataUpdate( $this->dataProviderId, __FUNCTION__, $params, self::MEDIUM_PRIORITY_QUEUE_ID );

            return true;
        }
        else
        {
            // get the product id to update from the parameters
            if( isset( $params['product_id'] ) )
            {
                $inputProductId = $params['product_id'];
            }
            else
            {
                // if a message doesn't give a product id
                $this->debugMsg( 'no product id given' );

                return false;
            }

            // get whether or not to force an update from the parameters
            if( isset( $params['force_update'] ) )
            {
                $forceUpdate = $params['force_update'];
            }
            else
            {
                // if it's not set go with no
                $forceUpdate = false;
            }

            $inputProduct = Product::model()
                                   ->findByPk( $inputProductId );
            // get the input product's name
            if( $inputProduct == null )
            {
                $this->debugMsg( 'product does not exist for product id ' . $inputProductId );

                return false;
            }

            // get the root category id
            $rootCategoryId = $this->db->createCommand( "SELECT c.root_category_id FROM category_product cp
 JOIN category c ON cp.category_id=c.category_id WHERE cp.product_id=:productId AND c.root_category_id IN
 (" . $this->_productAssemblerService->getRootCategoriesCSV( 'GB' ) . ") LIMIT 1;" )
                                       ->queryScalar( array(":productId" => $inputProductId) );

            if( $rootCategoryId != $this->booksRootCategoryId && $rootCategoryId != $this->videoGamesRootCategoryId )
            {
                // if it doesn't have a root category at all
                if( $rootCategoryId === false )
                {
                    $this->debugMsg( 'no root category for product id ' . $inputProductId );
                    // queue it to the assembler which should handle this
                    $this->_productAssemblerService->assembleProductByProductId( $inputProductId, true );
                }
                // if it has an invalid root category
                else
                {
                    $this->debugMsg( 'invalid root category for product id ' . $inputProductId );
                }

                return false;
            }
            //find current Product Relations
            $this->relations = $this->getProductRelations( $inputProductId );

            if( isset( $this->relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] ) )
            {
                // if it's already conflicted leave it alone for now but queue it to have the conflict resolved
                if( $queueDownstream )
                {
                    $this->resolveConflictByConflictedProductId( $inputProductId, true );
                }

                return false;
            }

            // update the product if it needs it or if forced to
            if( $forceUpdate || $this->last_updated < time() - self::ONE_WEEK_SECONDS )
            {
                // get an array of known duplicates
                $this->debugMsg( $this->relations );
                $knownDuplicateIdsArray = ( !empty( $this->relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) ? $this->relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] : array() );

                // find products that are duplicates of this one
                $foundDuplicateIdsArray = $this->findDuplicates( $inputProductId, $rootCategoryId, $knownDuplicateIdsArray );

                if( !empty( $foundDuplicateIdsArray ) )
                {
                    // if we found any new duplicates
                    // get details of what products already are or have masters
                    $masterProductsReport = $this->createMasterProductsReport( $foundDuplicateIdsArray );

                    //if the product is a master
                    if( isset( $this->relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) || empty( $this->relations[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) )
                    {
                        // if this product is a master
                        foreach( $masterProductsReport['master_product_ids'] as $duplicateMasterProductId )
                        {
                            if( ( $key = array_search( $duplicateMasterProductId, $foundDuplicateIdsArray ) ) !== false )
                            {
                                // remove other master products from the duplicate list
                                unset( $foundDuplicateIdsArray[$key] );
                            }

                            /**
                             * @todo review
                             */
                            if( empty( $this->relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] or !in_array( $duplicateMasterProductId, $this->relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) ) )
                            {
                                // set the products as duplicates of each other if they aren't already
                                $this->createDuplicateRelationship( $duplicateMasterProductId, $inputProductId, self::MASTER_MASTER_RELATIONSHIP );
                            }
                            if( $queueDownstream )
                            {
                                // queue conflict resolution (queue downstream will only be off if this method is called as part of conflict resolution)
                                $this->resolveConflictByMasterIds( $inputProductId, $duplicateMasterProductId, true );
                            }
                        }

                        foreach( $masterProductsReport['subordinate_products'] as $subordinateDuplicateProduct )
                        {
                            if( ( $key = array_search( $subordinateDuplicateProduct['product_id'], $foundDuplicateIdsArray ) ) !== false )
                            {
                                // remove products that are already subordinates from the list
                                unset( $foundDuplicateIdsArray[$key] );
                            }
                            // if they aren't subordinates of this product and haven't had a conflict resolved against this master then make them conflicted with this and their old master
                            if( $this->createDuplicateRelationship( $subordinateDuplicateProduct['product_id'], $inputProductId, self::SUBORDINATE_MASTER_RELATIONSHIP ) && $queueDownstream )
                            {
                                // queue conflict resolution (queue downstream will only be off if this method is called as part of conflict resolution)
                                $this->resolveConflictByConflictedProductId( $subordinateDuplicateProduct['product_id'], true );
                            }

                        }

                        foreach( $masterProductsReport['conflicted_products'] as $conflictedDuplicateProduct )
                        {
                            if( ( $key = array_search( $conflictedDuplicateProduct['product_id'], $foundDuplicateIdsArray ) ) !== false )
                            {
                                // remove products that are already conflicted form the list
                                unset( $foundDuplicateIdsArray[$key] );
                            }
                            // if they aren't subordinates of this product and haven't had a conflict resolved against this master then make them conflicted with this and their old master
                            if( $this->createDuplicateRelationship( $conflictedDuplicateProduct['product_id'], $inputProductId, self::SUBORDINATE_MASTER_RELATIONSHIP ) && $queueDownstream )
                            {
                                // queue conflict resolution (queue downstream will only be off if this method is called as part of conflict resolution)
                                $this->resolveConflictByConflictedProductId( $conflictedDuplicateProduct['product_id'], true );
                            }

                        }

                        foreach( $foundDuplicateIdsArray as $foundDuplicateId )
                        {
                            // add products left on the list as subordinates of this product
                            $this->createDuplicateRelationship( $inputProductId, $foundDuplicateId, self::SUBORDINATE_MASTER_RELATIONSHIP, true );
                        }

                        if( $queueAssembler && !empty( $foundDuplicateIdsArray ) )
                        {
                            // if there are any products left (new duplicates that will actually be used) queue this product to the assembler
                            $this->_productAssemblerService->assembleProductByProductId( $inputProductId, true );
                        }

                        $this->debugMsg( $inputProductDuplicationDataProviderProduct, 3 );
                    }
                    else
                    {
                        // if this product is either new or was unique but we've now found some duplicates
                        foreach( $masterProductsReport['master_product_ids'] as $duplicateMasterProductId )
                        {
                            if( ( $key = array_search( $duplicateMasterProductId, $foundDuplicateIdsArray ) ) !== false )
                            {
                                // remove any master products from the list of duplicates
                                unset( $foundDuplicateIdsArray[$key] );
                            }
                        }

                        $masterCount = count( $masterProductsReport['master_product_ids'] );

                        if( $masterCount > 1 )
                        {
                            $successCount = 0;
                            // if we're a duplicate of more than one master mark this product as conflicted between them
                            foreach( $masterProductsReport['master_product_ids'] as $aMasterProductId )
                            {
                                if( $this->createDuplicateRelationship( $aMasterProductId, $inputProductDuplicationDataProviderProduct, self::SUBORDINATE_MASTER_RELATIONSHIP ) )
                                {
                                    $successCount += 1;
                                }
                            }

                            if( $successCount >= 2 && $queueDownstream )
                            {
                                // and queue it for conflict resolution (unless set not to)
                                $this->resolveConflictByConflictedProductId( $inputProductId, true );
                            }
                        }
                        elseif( $masterCount == 1 )
                        {
                            // if there's only one master mark this product as a subordinate of it
                            $this->createDuplicateRelationship( $masterProductsReport['master_product_ids'][0], $inputProductDuplicationDataProviderProduct, self::SUBORDINATE_MASTER_RELATIONSHIP );
                        }
                        else
                        {
                            // if this product isn't a duplicate of any masters but is a duplicate of some subordinates
                            if( !empty( $masterProductsReport['most_common_master_product_id'] ) )
                            {
                                // set it as subordinate to the master with the most subordinates among this product's duplicates
                                $this->createDuplicateRelationship( $masterProductsReport['most_common_master_product_id'], $inputProductDuplicationDataProviderProduct, self::SUBORDINATE_MASTER_RELATIONSHIP );
                            }
                            else
                            {
                                // if we can;t find any existing masters then make a new, blank, one
                                $masterProductDuplicationDataProviderProduct = $this->createNewMasterProduct( $inputProductId, $inputProductName );

                                // then mark this product as a duplicate of it
                                $this->createDuplicateRelationship( $masterProductDuplicationDataProviderProduct, $inputProductDuplicationDataProviderProduct, self::SUBORDINATE_MASTER_RELATIONSHIP );

                            }
                        }
//                        }

                        // if it isn't a subordinate it's conflicted so leave its duplicates alone until that has been resolved
                        $masterProductId = json_decode( $inputProductDuplicationDataProviderProduct->product_attributes, true )[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY];
                        /**
                         * @var DataProviderProduct $masterProductDuplicationDataProviderProduct
                         */
                        $masterProductDuplicationDataProviderProduct = DataProviderProduct::model()
                                                                                          ->findByAttributes( array(
                                                                                              'product_id'       => $masterProductId,
                                                                                              'data_provider_id' => $this->dataProviderId,
                                                                                          ) );

                        //for the products that are masters of other products
                        foreach( $masterProductsReport['master_product_ids'] as $foundMasterProductId )
                        {
                            // add them to our master (causing a conflict which we will then queue to resolve)
                            if( ( $key = array_search( $foundMasterProductId, $foundDuplicateIdsArray ) ) !== false )
                            {
                                if( $this->createDuplicateRelationship( $inputProduct->getPrimaryKey(), $foundMasterProductId, self::CONFLICTED_MASTER_RELATION_ID ) && $queueDownstream )
                                {
                                    $this->resolveConflictByConflictedProductId( $inputProduct->getPrimaryKey(), true );
                                }
                                unset( $foundDuplicateIdsArray[$key] );
                            }
                        }

                        // for the products that are subordinates of other masters (but not currently conflicted)
                        foreach( $masterProductsReport['subordinate_products'] as $subordinateDuplicateProduct )
                        {
                            // add them to our master (causing a conflict which we will then queue to resolve)
                            if( ( $key = array_search( $subordinateDuplicateProduct['product_id'], $foundDuplicateIdsArray ) ) !== false )
                            {
                                if( $this->createDuplicateRelationship( $inputProduct->getPrimaryKey(), $subordinateDuplicateProduct['product_id'], self::SUBORDINATE_MASTER_RELATIONSHIP ) && $queueDownstream )
                                {
                                    $this->resolveConflictByConflictedProductId( $subordinateDuplicateProduct['product_id'], true );
                                }
                                unset( $foundDuplicateIdsArray[$key] );
                            }
                        }

                        // for the products that are already conflicted between masters
                        foreach( $masterProductsReport['conflicted_products'] as $conflictedDuplicateProduct )
                        {
                            // add them to our master (causing a conflict which we will then queue to resolve)
                            if( ( $key = array_search( $conflictedDuplicateProduct['product_id'], $foundDuplicateIdsArray ) ) !== false )
                            {
                                if( $this->createDuplicateRelationship( $inputProduct->getPrimaryKey(), $conflictedDuplicateProduct['product_id'], self::CONFLICTED_MASTER_PRODUCT_IDS_KEY ) && $queueDownstream )
                                {
                                    $this->resolveConflictByConflictedProductId( $conflictedDuplicateProduct['product_id'], true );
                                }
                                unset( $foundDuplicateIdsArray[$key] );
                            }
                        }

                        // for the products that are currently unique
                        foreach( $foundDuplicateIdsArray as $foundDuplicateId )
                        {
                            // mark any other products as duplicates of this product's master
                            $this->createDuplicateRelationship( $inputProduct->getPrimaryKey(), $foundDuplicateId, self::SUBORDINATE_MASTER_RELATIONSHIP, true );
                        }

                        if( $queueAssembler )
                        {
                            // queue this product's master to the assembler (as we've added products to it)
                            $this->_productAssemblerService->assembleProductByProductId( $masterProductId, true );
                        }

                        // set this product as having been updated
                        $inputProductDuplicationDataProviderProduct->setAttribute( 'last_updated', time() );
                        if( $inputProductDuplicationDataProviderProduct->product_name == $this->_newName )
                        {
                            // if this product was new and hasn't been marked as a duplicate then marks it as unique
                            $inputProductDuplicationDataProviderProduct->setAttribute( 'product_name', self::UNIQUE_PRODUCT_NAME );
                        }

                        // save it
                        $inputProductDuplicationDataProviderProduct->save();

                        $this->debugMsg( $inputProductDuplicationDataProviderProduct, 3 );

                        $this->messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $inputProductId, true );
                    }
                }

            }
        }
    }

    /**
     * This function gets a list of product Ids that are already known as duplicates of this product
     *
     * @param int|bool $productId
     *
     * @return array
     */
    public function getKnownDuplicateIds( $productId = false )
    {
        $knownDuplicateIdsArray = array();
        $masterProductIds = array();
        $needsCleanup = false;
        $relations = $this->getProductRelations( $productId );
        if( isset( $relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
        {
            // get the ids of products subordinate to this master and masters that are duplicates of this one.
            foreach( $relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] as $subordinate )
            {
                //the master in a relation is always the product id, so get all records where this product matches the product Id
                if( !in_array( $subordinate, $knownDuplicateIdsArray ) )
                $knownDuplicateIdsArray[] = $subordinate;

            }

        }
        if( isset( $relations[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) )
        {
            // get the ids of products subordinate to this master and masters that are duplicates of this one.
            foreach( $relations[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] as $master )
            {
                //the master in a relation is always the product id, so get all records where this product matches the product Id
                if( !in_array( $master, $masterProductIds ) )
                {
                    $masterProductIds[] = $master;
                }

            }

        }

        if( isset( $relations[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] ) )
        {
            //get the ida of any masters this product used to be conflicted under
            foreach( $relations[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] as $notDuplicate )
            {
                if( !in_array( $notDuplicate, $masterProductIds))
                $masterProductIds[] = $notDuplicate;
            }
        }
        if( isset( $relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] ) )
        {
            foreach( $relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] as $conflictedRelation )
            {
                if( !in_array( $conflictedRelation, $masterProductIds))
                $masterProductIds[] = $conflictedRelation;
            }
        }

        if( isset( $masterProductIds ) )
        {
            // get the ids of those master's subordinates
            foreach( $masterProductIds as $masterProductId )
            {
                // add the master's id to the list of known ids
                if( !in_array( $masterProductId, $knownDuplicateIdsArray ) )
                    $knownDuplicateIdsArray[] = $masterProductId;

                // get the master's related products
                /**
                 * @var DataProviderProduct $masterProductDuplicationDataProviderProduct
                 */
                if( $this->checkIsMaster( $masterProductId ) )
                {

                    // add the master's subordinate ids to the list of ids.
                    $knownDuplicateIdsArray = array_merge( $knownDuplicateIdsArray, $this->getMasterProductKnownDuplicates( $masterProductId ) );
                }
                else
                {
                    // if we need to cleanup the product (because on of it's masters is missing) set needs cleanup tp true then exit the loop and switch
                    $needsCleanup = true;
                }
            }

        }
        foreach( $knownDuplicateIdsArray as $key => $value )
        {
            if( $value == $productId || in_array( $value, $masterProductIds ) )
            {
                unset( $knownDuplicateIdsArray[$key] );
            }
        }
        // if the product needs to be repaired attempt to do so
        if( $needsCleanup )
        {
            if( $this->repairDuplicateRelationships( $productId ) )
            {
                // if the repair went well then try again with the repaired object
                $knownDuplicateIdsArray = $this->getKnownDuplicateIds( $inputProductDuplicationDataProviderProduct );
            }
            else
            {
                // if it didn't return an empty array
                $knownDuplicateIdsArray = array();
            }
        }


        return $knownDuplicateIdsArray;
    }

    /**
     * Function gets a list of products that are known duplicates of a master product
     *
     * @param int  $productId
     * @param bool $recurse
     *
     * @return array
     */
    public function getMasterProductKnownDuplicates( $productId, $recurse = false )
    {

        $knownDuplicateIdsArray = array();

        //get all the relations
        $productRelations = $this->getProductRelations( $productId );

        // get the lists of duplicates
        if( isset( $productRelations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
        {
            $knownDuplicateIdsArray = array_merge( $knownDuplicateIdsArray, $productRelations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] );

        }


        // get the lists of known not duplicates
        // masters may have a list of product ids that look like duplicates but aren't so get them
        // (they'll be excluded from the duplicate detection)
        // if there is a master product that is set as a duplicate then add it to the
        if( isset( $productRelations[self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID] ) )
        {
            foreach( $productRelations[self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID] as $relatedProductId )
            {

                $knownDuplicateIdsArray[] = $relatedProductId;
                if( $recurse )
                {
                    // don't set that to recurse so we only ever do a maximum of a single level of recursion
                    $knownDuplicateIdsArray = array_merge( $knownDuplicateIdsArray, $this->getMasterProductKnownDuplicates( $relatedProductId ) );
                }
            }
        }
        return $knownDuplicateIdsArray;
    }

    /**
     * function creates a new product to act as a master for a given product
     *
     * @param int    $subordinateProductId
     * @param string $subordinateProductName
     *
     * @return bool|int
     */
    public function createNewMasterProduct( $subordinateProductId, $subordinateProductName )
    {
        $this->debugMsg( "creating new master product" );
        $masterProduct = new Product();

        // create the new product
        $masterProduct->setAttributes( array(
            'last_updated' => 0,
            'product_name' => $subordinateProductName,
        ), false );

        // save the new product to get a product_id for it
        if( !$masterProduct->save() )
        {
            return false;
        }

        $this->debugMsg( "created product with product_id : " . $masterProduct->product_id );
        $this->debugMsg( $masterProduct, 3 );

        /**
         * get the categories of the product we are making the master for
         * @var $subordinateCategoryProductsArray CategoryProduct[]
         */
        $subordinateCategoryProductsArray = CategoryProduct::model()
                                                           ->findAllByAttributes( array(
                                                               'product_id' => $subordinateProductId
                                                           ) );

        /**
         * @var $masterCategoryProductsArray CategoryProduct[]
         */
        $masterCategoryProductsArray = array();

        $this->debugMsg( $subordinateCategoryProductsArray );
        foreach( $subordinateCategoryProductsArray as $key => $subordinateCategoryProduct )
        {
            // for each category the input product is in create a category_product to link the master to that category.
            $this->debugMsg( "creating category product for category : " . $subordinateCategoryProduct->category_id, 2 );
            $masterCategoryProductsArray[$key] = new CategoryProduct();
            $masterCategoryProductsArray[$key]->setAttributes( array(
                'product_id'  => $masterProduct->product_id,
                'category_id' => $subordinateCategoryProduct->category_id,
            ), false );
            $masterCategoryProductsArray[$key]->save();
        }

        $this->debugMsg( "created " . count( $masterCategoryProductsArray ) . " category_products" );

        $this->debugMsg( $masterCategoryProductsArray, 3 );

        // finally create a ProductDuplicationDataProviderProduct for the master

        $this->debugMsg( 'Creating new master ProductDuplicationDataProviderProduct' );
        $productRelation = new ProductRelation();
        $productRelation->setAttributes( array(
            'product_id'        => $masterProduct->product_id,
            'relationship_type' => self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID,
            'related_product'   => $subordinateProductId,
        ) );

        $this->debugMsg( $productRelation->getAttributes() );
        $productRelation->save();

        $this->debugMsg( $productRelation->getAttributes() );
        $this->debugMsg( 'Created Relation' );

        return $productRelation;
    }

    /**
     * function creates an array containing details of what products with ids in $duplicateProductIdsArray are already marked as masters/subordinates/conflicted
     *
     * @param array $duplicateProductIdsArray
     *
     * @return array
     */
    public function createMasterProductsReport( array $duplicateProductIdsArray )
    {
        $this->debugMsg( $duplicateProductIdsArray );
        $this->debugMsg( "creating master products report" );
        $masterProductsReport = array();

        // get the ids of products that are master products
        $masterProductsReport['master_product_ids'] = $this->findMasterDuplicateProducts( $duplicateProductIdsArray );

        // get details of products that are subordinate products already
        $masterProductsReport['subordinate_products'] = $this->findSubordinateDuplicateProducts( $duplicateProductIdsArray );

        // get details of products that are marked as not duplicate
        $masterProductsReport['not_duplicate_products'] = $this->findNotDuplicateProducts( $duplicateProductIdsArray );

        $subordinateProductMasterIds = array();

        // create a list of master products of products that are subordinates already
        foreach( $masterProductsReport['subordinate_products'] as $subordinateDuplicateProduct )
        {
            if( isset( $subordinateProductMasterIds[$subordinateDuplicateProduct[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY][0]] ) )
            {
                $subordinateProductMasterIds[$subordinateDuplicateProduct[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY][0]] += 1;
            }
            else
            {
                $subordinateProductMasterIds[$subordinateDuplicateProduct[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY][0]] = 1;
            }
        }

        // get the id of the product that is the master of the most subordinate products
        if( !empty( $subordinateProductMasterIds ) )
        {
            arsort( $subordinateProductMasterIds );

            $masterProductsReport['most_common_master_product_id'] = key( $subordinateProductMasterIds );
            $this->debugMsg( "most common master product id : " . $masterProductsReport['most_common_master_product_id'] );
        }

        // get details of products that are already conflicted
        $masterProductsReport['conflicted_products'] = $this->findConflictedDuplicateProducts( $duplicateProductIdsArray );

        return $masterProductsReport;
    }

    public function findNotDuplicateProducts( $duplicateProductIdsArray )
    {
        $notDuplicateProducts = array();
        if( !empty( $duplicateProductIdsArray ) )
        {
            $this->debugMsg( "Looking for conflicted products in duplicate product ids" );
            $notDuplicateProductsSQL = "SELECT product_id, related_product_id, relationship_type FROM product_relation WHERE (product_id IN(" . implode( ',', $duplicateProductIdsArray ) . ") or related_product_id in (" . implode( ',', $duplicateProductIdsArray ) . "))
        and relationship_type = " . self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID;

            $this->debugMsg( $notDuplicateProductsSQL, 2 );

            $notDuplicateProductsResult = $this->db->createCommand( $notDuplicateProductsSQL )
                                                   ->queryAll();

            foreach( $notDuplicateProductsResult as $notDuplicateProduct )
            {
                if( !in_array( $notDuplicateProduct['product_id'], $duplicateProductIdsArray ) )
                {
                    $productId = $notDuplicateProduct['product_id'];
                    $relatedProduct = $notDuplicateProduct['related_product_id'];
                }
                else
                {
                    $relatedProduct = $notDuplicateProduct['product_id'];
                    $productId = $notDuplicateProduct['related_product_id'];
                }
                $conflictedDuplicateProductsArray[$productId]['product_id'] = $productId;
                $conflictedDuplicateProductsArray[$productId][self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY][] = $relatedProduct;

            }

            $this->debugMsg( "found " . count( $conflictedDuplicateProductsArray ) . " conflicted products" );
            $this->debugMsg( $conflictedDuplicateProductsArray, 3 );
        }

        return $conflictedDuplicateProductsArray;
    }
    /**
     * function gets certain details about products in $duplicateProductIdsArray that are already marked as conflicted
     *
     * @param $duplicateProductIdsArray
     *
     * @return array
     */
    public function findConflictedDuplicateProducts( $duplicateProductIdsArray )
    {
        $conflictedDuplicateProductsArray = array();

        if( !empty( $duplicateProductIdsArray ) )
        {
            $this->debugMsg( "Looking for conflicted products in duplicate product ids" );
            $conflictedDuplicateProductsSQL = "SELECT product_id, related_product_id, relationship_type FROM product_relation WHERE (product_id IN(" . implode( ',', $duplicateProductIdsArray ) . ") or related_product_id in (" . implode( ',', $duplicateProductIdsArray ) . "))
        and relationship_type in (" . self::CONFLICTED_MASTER_RELATION_ID . ", " . self::CONFLICTED_RELATION_ID . ")";

            $this->debugMsg( $conflictedDuplicateProductsSQL, 2 );

            $conflictedDuplicateProductsResult = $this->db->createCommand( $conflictedDuplicateProductsSQL )
                                                          ->queryAll();

            foreach( $conflictedDuplicateProductsResult as $conflictedDuplicateProduct )
            {
                if( !in_array( $conflictedDuplicateProduct['product_id'], $duplicateProductIdsArray ) )
                {
                    $productId = $conflictedDuplicateProduct['product_id'];
                    $relatedProduct = $conflictedDuplicateProduct['related_product_id'];
                }
                else
                {
                    $relatedProduct = $conflictedDuplicateProduct['product_id'];
                    $productId = $conflictedDuplicateProduct['related_product_id'];
                }

                // format the result how we want it
                $conflictedDuplicateProductsArray[$productId]['product_id'] = $productId;
                $conflictedDuplicateProductsArray[$productId]['conflicting_master_ids'][] = $relatedProduct;
            }

            $this->debugMsg( "found " . count( $conflictedDuplicateProductsArray ) . " conflicted products" );
            $this->debugMsg( $conflictedDuplicateProductsArray, 3 );
        }

        return $conflictedDuplicateProductsArray;
    }

    /**
     * function gets certain details about products in $duplicateProductIdsArray that are already marked as subordinates
     *
     * @param $duplicateProductIdsArray
     *
     * @return array
     */
    public function findSubordinateDuplicateProducts( $duplicateProductIdsArray )
    {
        $subordinateDuplicateProductsArray = array();

        $this->debugMsg( "Looking for subordinate products in duplicate product ids" );

        if( !empty( $duplicateProductIdsArray ) )
        {

            $subordinateDuplicateProductsSQL = "SELECT product_id, related_product_id, relationship_type FROM product_relation WHERE related_product_id IN(" . implode( ',', $duplicateProductIdsArray ) . ") and
                        relationship_type  = " . SELF::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID;

            $this->debugMsg( $subordinateDuplicateProductsSQL, 2 );

            $subordinateDuplicateProductsResult = $this->db->createCommand( $subordinateDuplicateProductsSQL )
                                                           ->queryAll();

            foreach( $subordinateDuplicateProductsResult as $subordinateDuplicateProduct )
            {
                // format the results the way we want them.
                $subordinateDuplicateProductsArray[$subordinateDuplicateProduct['related_product_id']]['product_id'] = $subordinateDuplicateProduct['related_product_id'];
                $subordinateDuplicateProductsArray[$subordinateDuplicateProduct['related_product_id']][self::SUBORDINATE_MASTER_PRODUCT_ID_KEY][] = $subordinateDuplicateProduct['product_id'];
            }
            $this->debugMsg( "found " . count( $subordinateDuplicateProductsArray ) . " subordinate duplicate products" );

            $this->debugMsg( $subordinateDuplicateProductsArray, 3 );
        }

        return $subordinateDuplicateProductsArray;
    }

    /**
     * Function gets an array of product ids of master products from an array of product ids that might be for master products
     *
     * @param array $duplicateProductIdsArray
     *
     * @return array|CDbDataReader
     */
    public function findMasterDuplicateProducts( array $duplicateProductIdsArray )
    {
        $this->debugMsg( "Looking for master products in duplicate product ids" );
        $masterIdsArray = array();
        foreach( $duplicateProductIdsArray as $potentialMasterProductId )
        {
            if( $this->checkIsMaster( $potentialMasterProductId ) )
            {
                $masterIdsArray[] = $potentialMasterProductId;
            }
        }

        return $masterIdsArray ?: array();
    }

    /**
     * This function orchestrates the identification of duplicate products.
     *
     * @param int   $inputProductId
     * @param int   $rootCategoryId
     * @param array $knownIdsArray
     *
     * @return array
     */
    public function findDuplicates( $inputProductId, $rootCategoryId, array $knownIdsArray = array() )
    {
        $this->debugMsg( 'Finding Duplicates for product Id : ' . $inputProductId );
        $duplicateProductIdsArray = array();

        // if it's a book product
        if( $rootCategoryId == $this->booksRootCategoryId )
        {
            // get the amazon alternate versions
            $amazonDuplicates = $this->getAmazonAlternateVersions( $inputProductId, $knownIdsArray );

            // add these to the known and found duplicates
            $knownIdsArray += $amazonDuplicates;
            $duplicateProductIdsArray += $amazonDuplicates;
        }

        // get the details necessary for duplicate detection
        $inputProduct = $this->getInputProductDetailsForDuplicateDetection( $inputProductId );

        $potentialDuplicateProductIdsArray = array();

        // get a first guess of what products may be duplicates
        $potentialDuplicateProductsArray = $this->getPotentialDuplicateProducts( $inputProductId, $inputProduct, $potentialDuplicateProductIdsArray, $rootCategoryId, $knownIdsArray );

        // filter out products that have names that are not similar enough to the assembler product and score those that remain
        $this->filterPotentialDuplicatesByName( $inputProduct, $potentialDuplicateProductsArray, $potentialDuplicateProductIdsArray );

        // if there are no remaining duplicates then bail
        if( empty( $potentialDuplicateProductIdsArray ) )
        {
            $this->debugMsg( "Finished with " . count( $duplicateProductIdsArray ) . " duplicates found" );

            return $duplicateProductIdsArray;
        }

        $this->debugMsg( $potentialDuplicateProductIdsArray );

        // score products based on the percentage of categories they have in common
        $this->scorePotentialDuplicatesByCategory( $inputProduct, $potentialDuplicateProductsArray );

        // score products based on their release date
        if( !empty( $inputProduct['release_date'] ) )
        {
            $this->scorePotentialDuplicatesByReleaseDate( $inputProduct, $potentialDuplicateProductsArray, $rootCategoryId );
        }

        // get the necessary attributes for comparison
        $this->preparePotentialDuplicatesAttributes( $inputProduct, $potentialDuplicateProductsArray, $inputProductId, $potentialDuplicateProductIdsArray, $rootCategoryId );

        if( !empty( $inputProduct['product_attributes'] ) )
        {
            // score products based on comparison of their attributes
            $this->scorePotentialDuplicatesByAttributes( $inputProduct, $potentialDuplicateProductsArray );
        }

        // choose products based on some threshold score
        $duplicateProductIdsArray += $this->decideDuplicateProducts( $potentialDuplicateProductsArray, $potentialDuplicateProductIdsArray );

        // discard products with too low a score
        $this->debugMsg( "discarding potential duplicates by score" );
        foreach( $potentialDuplicateProductsArray as $key => $potentialDuplicateProduct )
        {
            if( $potentialDuplicateProduct['duplicate_score'] <= $potentialDuplicateProduct['max_score'] * $this->_minimumPercentScore )
            {
                $this->debugMsg( "discarding Id : " . $potentialDuplicateProduct['product_id'], 2 );
                unset( $potentialDuplicateProductsArray[$key], $potentialDuplicateProductIdsArray[$potentialDuplicateProduct['product_id']] );
            }
        }

        // if all products have been either chosen or discarded then finish
        if( empty( $potentialDuplicateProductsArray ) )
        {
            $this->debugMsg( "Finished with " . count( $duplicateProductIdsArray ) . " duplicates found" );

            return $duplicateProductIdsArray;
        }

        // choose products that have nigh identical ratings
        $this->findPotentialDuplicatesBySimilarRatings( $potentialDuplicateProductsArray, $potentialDuplicateProductIdsArray, $duplicateProductIdsArray, $inputProductId );

        // if all products have been either chosen or discarded then finish
        if( empty( $potentialDuplicateProductsArray ) )
        {
            $this->debugMsg( "Finished with " . count( $duplicateProductIdsArray ) . " duplicates found" );

            return $duplicateProductIdsArray;
        }

        // choose products that have nigh identical reviews
        $this->findPotentialDuplicatesBySimilarReviews( $potentialDuplicateProductsArray, $potentialDuplicateProductIdsArray, $duplicateProductIdsArray, $inputProductId );

        // if all products have been either chosen or discarded then finish
        if( empty( $potentialDuplicateProductsArray ) )
        {
            $this->debugMsg( "Finished with " . count( $duplicateProductIdsArray ) . " duplicates found" );

            return $duplicateProductIdsArray;
        }

        // choose products that have nigh identical descriptions
        $this->findPotentialDuplicatesBySimilarDescriptions( $potentialDuplicateProductsArray, $potentialDuplicateProductIdsArray, $duplicateProductIdsArray, $inputProduct );

        // all checks I've considered have been completed so finish
        $this->debugMsg( "remaining potential duplicate products : " . implode( ' ', $potentialDuplicateProductIdsArray ), 2 );
        $this->debugMsg( "Finished with " . count( $duplicateProductIdsArray ) . " duplicates found" );

        return $duplicateProductIdsArray;

    }

    /**
     * Function finds the products that correspond to the Amazon alternate versions of the input product
     *
     * @param int   $inputProductId
     * @param array $knownProductIdsArray
     *
     * @return array
     */
    public function getAmazonAlternateVersions( $inputProductId, $knownProductIdsArray = array() )
    {
        // get the amazon data provider id
        $amazonDataProviderId = DataProvider::model()
                                            ->getIdByName( self::AMAZON_DATA_PROVIDER_NAME );

        $amazonAlternateProductIdsArray = array();

        // query the database for the product attributes of an amazon data provider product for the input product
        // json_decode the result as an array and assign it to $amazonDataProviderProductAttributes
        // if that was 'truthy'
        if( $amazonDataProviderProductAttributes = json_decode( $this->db->createCommand( 'SELECT product_attributes FROM data_provider_product WHERE product_id = :productId AND data_provider_id = :dataProviderId' )
                                                                         ->queryScalar( array(
                                                                             ':productId'      => $inputProductId,
                                                                             ':dataProviderId' => $amazonDataProviderId
                                                                         ) ), true )
        )
        {
            $this->debugMsg( 'Product is a book with an Amazon data provider', 2 );

            // if there are alternate versions
            if( isset( $amazonDataProviderProductAttributes['AlternateVersions'] ) )
            {
                $this->debugMsg( 'Product has Amazon Alternate Versions' );

                // grab them
                $alternateVersionsArray = $amazonDataProviderProductAttributes['AlternateVersions'];

                // if they're nested
                if( isset( $alternateVersionsArray['AlternateVersions'] ) )
                {
                    // fix that
                    $alternateVersionsArray = $alternateVersionsArray['AlternateVersions'];
                }

                // if they're nested in a different way
                if( isset( $alternateVersionsArray['AlternateVersion'] ) )
                {
                    // fix that too
                    $alternateVersionsArray = $alternateVersionsArray['AlternateVersion'];
                }
                $this->debugMsg( "Looking for " . count( $alternateVersionsArray ) . " alternate versions", 2 );

                // create a MySQL query to find the product indicated by amazon
                $alternateProductIdsCommand = $this->db->createCommand( "SELECT product_id FROM data_provider_product WHERE data_provider_product_id = :asin AND data_provider_id = :dataProviderId" . ( ( $knownProductIdsArray && false ) ? ( " AND NOT product_id IN (" . implode( ',', $knownProductIdsArray ) . ")" ) : ';' ) );

                // for each amazon alternate verson
                foreach( array_column( $alternateVersionsArray, 'ASIN' ) as $asin )
                {
                    // query the database for the product id and assign the result to $alternateProductId
                    // if we got a result
                    if( ( $alternateProductId = $alternateProductIdsCommand->queryScalar( array(':asin' => $asin, ':dataProviderId' => $amazonDataProviderId) ) ) )
                    {
                        $this->debugMsg( "Found duplicate : $alternateProductId : from ASIN : $asin", 3 );

                        // if the product is a book (it might be a kindle book, which we don't want right now)
                        if( Product::model()
                                   ->getRootCategoryId( $alternateProductId ) == $this->booksRootCategoryId && $alternateProductId != $inputProductId
                        )
                        {
                            // add it to the array
                            $amazonAlternateProductIdsArray[] = $alternateProductId;
                        }
                    }
                    else
                    {
                        $this->debugMsg( "missing duplicate for ASIN: $asin", 3 );
                    }
                }
                $this->debugMsg( "Found " . count( $amazonAlternateProductIdsArray ) . " alternate versions", 2 );

                // discard all products that were already known
                $amazonAlternateProductIdsArray = array_diff( $amazonAlternateProductIdsArray, $knownProductIdsArray );
                $this->debugMsg( "Found " . count( $amazonAlternateProductIdsArray ) . " new alternate versions" );
            }
        }

        $this->debugMsg( $amazonAlternateProductIdsArray );
        // return the array
        return $amazonAlternateProductIdsArray;
    }

    /**
     * this function gets the required fields for duplicate detection, using those supplied if possible (otherwise it uses database fields)
     *
     * @param int $inputProductId
     *
     * @return array
     */
    public function getInputProductDetailsForDuplicateDetection( $inputProductId )
    {
        $this->debugMsg( "Getting details on input product" );

        if( !is_numeric( $inputProductId ) )
        {
            $this->debugMsg( 'productId not numeric' );

            return false;
        }
        // get the necessary fields from the database
        $inputProductSQL = "SELECT p.product_id, p.product_name, p.product_description, p.product_name_soundex_lookup, p.is_subproduct, COUNT(DISTINCT cp.category_id) AS num_categories, p.release_date
FROM product p JOIN category_product cp ON p.product_id=cp.product_id WHERE p.product_id = " . $inputProductId . " GROUP BY p.product_id;";

        $this->debugMsg( $inputProductSQL, 2 );

        $inputProduct = $this->db->createCommand( $inputProductSQL )
                                 ->queryRow();

        // create the array used to compare product names
        $this->createProductNameComparison( $inputProduct );

        $this->debugMsg( 'Finished getting details on input product', 2 );
        $this->debugMsg( print_r( $inputProduct, true ), 3 );

        return $inputProduct;
    }

    /**
     * this function creates an array of words used to compare product names
     *
     * @param $product
     */
    public function createProductNameComparison( &$product )
    {
        /*
         * @todo: better recognition of words that mean the same thing (I've done what I've thought of)
         *
         * maybe move this to TextService
         */
        $this->debugMsg( "creating name comparison for product : " . $product['product_id'], 2 );

        /*
         * str_replace (turn '&' to 'and', have to do this first or the '&' will be removed)
         * preg_replace (turn invalid characters into whitespace) -> preg_replace (remove double spaces) ->
         * strtolower (make it all lower case so it's case insensitive) -> trim(remove whitespace from start and end)->explode (make it easy to compare word by word) ->
         * array_diff (remove words that we don't want to take into account) -> array_values ( to re-number the keys without re ordering them)
         */

        $product['name_comparison'] = array_slice( array_diff( explode( ' ', trim( strtolower( preg_replace( '/\s+/', ' ', preg_replace( '/[^\sa-zA-Z0-9]+/', ' ', str_replace( '&', 'and', $product['product_name'] ) ) ) ) ) ), $this->_nameStopWords ), 0 );

        // check each word against a et of lists of equivalent words (mainly numbers)
        foreach( $product['name_comparison'] as $key => $word )
        {
            if( isset( $this->_romanNumerals[$word] ) )
            {
                $product['name_comparison'][$key] = $this->_romanNumerals[$word];
            }
            elseif( isset( $this->_englishNumerals[$word] ) )
            {
                $product['name_comparison'][$key] = $this->_englishNumerals[$word];
            }

        }
        // also record the count
        $product['name_comparison_count'] = count( $product['name_comparison'] );
        $this->debugMsg( "created name array count : " . $product['name_comparison_count'], 2 );

    }

    /**
     * function gets first guess duplicate products and some details about them for later comparison
     *
     * @param int   $inputProductId
     * @param array $inputProduct
     * @param array $comparisonProductIdsArray
     * @param int   $rootCategoryId
     * @param array $exclusionIdsArray
     *
     * @return array
     */
    public function getPotentialDuplicateProducts( $inputProductId, $inputProductSoundexLookup, array &$comparisonProductIdsArray, $rootCategoryId, array $exclusionIdsArray = array() )
    {
        $this->debugMsg( 'Getting potential duplicates' );

        $exclusionIdsArray[] = $inputProductId;

        /* old, force category version (keeping for now in case I want to switch back to it. (15/07/15)
        $comparisonProductsSQL = "SELECT 0 AS duplicate_score, 0 AS max_score, p.product_id, p.product_name, p.product_description, p.product_name_soundex_lookup,
COUNT(DISTINCT cp1.category_id) AS num_shared_categories, COUNT(DISTINCT cp3.category_id) AS num_categories, p.release_date FROM product p
JOIN category_product cp1 ON p.product_id = cp1.product_id JOIN category_product cp2 ON cp1.category_id=cp2.category_id JOIN category_product cp3 ON p.product_id=cp3.product_id
WHERE p.product_id NOT IN (" . implode( ',', $exclusionIdsArray ) . ") AND cp2.product_id = $inputProductId
AND p.product_name_soundex_lookup = \"" . $inputProduct['product_name_soundex_lookup'] . "\" GROUP BY p.product_id;";
*/
        // get the data from the database
        $comparisonProductsSQL = "SELECT p.product_id, p.product_name, p.product_description, p.product_name_soundex_lookup,
COUNT(DISTINCT cp2.category_id) AS num_shared_categories, COUNT(DISTINCT cp1.category_id) AS num_categories, p.release_date FROM product p
JOIN category_product cp1 ON p.product_id = cp1.product_id LEFT JOIN category_product cp2 ON cp1.category_id=cp2.category_id AND cp2.product_id = $inputProductId
JOIN category c ON cp1.category_id = c.category_id WHERE p.product_id NOT IN (" . implode( ',', $exclusionIdsArray ) . ")
AND p.product_name_soundex_lookup = \"" . $inputProductSoundexLookup . "\" AND c.root_category_id = $rootCategoryId GROUP BY p.product_id;
";

        $this->debugMsg( $comparisonProductsSQL, 2 );

        $comparisonProductsResult = $this->db->createCommand( $comparisonProductsSQL )
                                             ->queryAll();

        // format the data to be how we want it
        $comparisonProductsArray = array();
        foreach( $comparisonProductsResult as $comparisonProduct )
        {
            $this->createProductNameComparison( $comparisonProduct );
            $comparisonProductsArray['product_id:' . $comparisonProduct['product_id']] = $comparisonProduct + array('duplicate_score' => 0, 'max_score' => 0);
            $comparisonProductIdsArray[$comparisonProduct['product_id']] = $comparisonProduct['product_id'];
        }

        $this->debugMsg( "Found " . count( $comparisonProductIdsArray ) . " potential product duplicates" );
        $this->debugMsg( "IDs : " . implode( ' ', $comparisonProductIdsArray ), 2 );
        $this->debugMsg( print_r( $comparisonProductsArray, true ), 3 );

        return $comparisonProductsArray;
    }

    /**
     * function filters and scores potential duplicate products by their names
     *
     * @param array $assemblerProduct
     * @param array $potentialDuplicateProductsArray
     * @param array $comparisonProductIdsArray
     */
    public function filterPotentialDuplicatesByName( array $assemblerProduct, array &$potentialDuplicateProductsArray, array &$comparisonProductIdsArray )
    {
        $this->debugMsg( "Filtering potential duplicate by name. Input name : " . $assemblerProduct['product_name'] );

        foreach( $potentialDuplicateProductsArray as $potentialDuplicateProductIdentifier => $potentialDuplicateProduct )
        {
            $this->debugMsg( "filtering product: " . $potentialDuplicateProduct['product_id'] . " :by name : " . $potentialDuplicateProduct['product_name'], 2 );
            // calculate the length of the shorter name
            $shortCount = min( $potentialDuplicateProduct['name_comparison_count'], $assemblerProduct['name_comparison_count'] );

            // compare the first section of the names (up to the length of the shorter name) and if they're different unset the potential product and move on to the next
            if( array_slice( $assemblerProduct['name_comparison'], 0, $shortCount ) != array_slice( $potentialDuplicateProduct['name_comparison'], 0, $shortCount ) )
            {
                $this->debugMsg( $assemblerProduct['name_comparison'] );
                $this->debugMsg( $potentialDuplicateProduct['name_comparison'] );
                $this->debugMsg( "removing product", 2 );
                unset( $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier], $comparisonProductIdsArray[$potentialDuplicateProduct['product_id']] );
                continue;
            }

            // if the names are the same length then they are exactly the same so score them as such and continue
            if( $potentialDuplicateProduct['name_comparison_count'] == $assemblerProduct['name_comparison_count'] )
            {
                $this->debugMsg( "Keeping product with score : " . $this->_duplicateScores['exact_name'], 2 );
                $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['duplicate_score'] += $this->_duplicateScores['exact_name'];
                $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['max_score'] += 100;
                continue;
            }

            // if the potential product's name is longer
            elseif( $potentialDuplicateProduct['name_comparison_count'] > $shortCount )
            {
                // if the next word is a number then it's probably a sequel so unset it and move on
                if( is_numeric( $potentialDuplicateProduct['name_comparison'][$shortCount] ) || $potentialDuplicateProduct['name_comparison'][$shortCount] == 'and' )
                {
                    $this->debugMsg( "removing product", 2 );
                    unset( $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier], $comparisonProductIdsArray[$potentialDuplicateProduct['product_id']] );
                    continue;
                }

                // if there is a word that is an indicator of an alternate edition e.g. 'edition' or 'classics' score it as such and continue
                if( array_intersect( $this->_editionNameIndicators, array_slice( $potentialDuplicateProduct['name_comparison'], $shortCount ) ) )
                {
                    $this->debugMsg( "Keeping product with score : " . $this->_duplicateScores['edition_name_suffix'], 2 );
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['duplicate_score'] += $this->_duplicateScores['edition_name_suffix'];
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['max_score'] += 100;
                    continue;
                }

                // if not then score it as such and continue
                else
                {
                    $this->debugMsg( "Keeping product with score : " . $this->_duplicateScores['other_name_suffix'], 2 );
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['duplicate_score'] += $this->_duplicateScores['other_name_suffix'];
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['max_score'] += 100;
                    continue;
                }
            }

            // if the assembler product's name is longer
            elseif( $assemblerProduct['name_comparison_count'] > $shortCount )
            {
                // if the next word is a number then it's probably a sequel so unset it and move on
                if( is_numeric( $assemblerProduct['name_comparison'][$shortCount] ) || $assemblerProduct['name_comparison'][$shortCount] == 'and' )
                {
                    $this->debugMsg( "removing product", 2 );
                    unset( $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier], $comparisonProductIdsArray[$potentialDuplicateProduct['product_id']] );
                    continue;
                }

                // if there is a word that is an indicator of an alternate edition e.g. 'edition' or 'classics' score it as such and continue
                if( array_intersect( $this->_editionNameIndicators, array_slice( $assemblerProduct['name_comparison'], $shortCount ) ) )
                {
                    $this->debugMsg( "Keeping product with score : " . $this->_duplicateScores['edition_name_suffix'], 2 );
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['duplicate_score'] += $this->_duplicateScores['edition_name_suffix'];
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['max_score'] += 100;
                    continue;
                }

                // if not then score it as such and continue
                else
                {
                    $this->debugMsg( "Keeping product with score : " . $this->_duplicateScores['other_name_suffix'], 2 );
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['duplicate_score'] += $this->_duplicateScores['other_name_suffix'];
                    $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['max_score'] += 100;
                    continue;
                }
            }

        }

        $this->debugMsg( count( $comparisonProductIdsArray ) . " potential duplicates remain after filtering by name" );
    }

    /**
     * this function scores potential products based on what percentage of categories they have in common with the assembler product
     *
     * @param array $assemblerProduct
     * @param array $potentialDuplicateProductsArray
     */
    public function scorePotentialDuplicatesByCategory( array $assemblerProduct, array &$potentialDuplicateProductsArray )
    {
        $this->debugMsg( "Scoring products by category" );
        foreach( $potentialDuplicateProductsArray as $potentialDuplicateProductIdentifier => $potentialDuplicateProduct )
        {
            $score = floor( 100 * $potentialDuplicateProduct['num_shared_categories'] / min( $potentialDuplicateProduct['num_categories'], $assemblerProduct['num_categories'] ) );
            $this->debugMsg( "scoring " . str_pad( $potentialDuplicateProduct['product_id'], 10, ' ' ) . "with score : $score", 2 );
            // score the product based on the percentage of shared categories/maximum possible shared categories
            $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['duplicate_score'] += $score;
            $potentialDuplicateProductsArray[$potentialDuplicateProductIdentifier]['max_score'] += 100;
        }
    }

    /**
     * this function scores potential products based on how close their release dates are
     *
     * @param array $potentialDuplicateProductsArray
     * @param array $assemblerProduct
     * @param int   $rootCategoryId
     */
    public function scorePotentialDuplicatesByReleaseDate( array $assemblerProduct, array &$potentialDuplicateProductsArray, $rootCategoryId )
    {
        $this->debugMsg( "scoring potential products by release date" );

        switch( $rootCategoryId )
        {
            case $this->moviesRootCategoryId:
                foreach( $potentialDuplicateProductsArray as $comparisonProductId => $comparisonProduct )
                {
                    if( !empty( $comparisonProduct['release_date'] ) )
                    {
                        // if the products have the same release date score it as such and move on
                        if( $comparisonProduct['release_date'] == $assemblerProduct['release_date'] )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['exact_date'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['exact_date'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they were released the same year score them as such and move on
                        elseif( abs( $comparisonProduct['release_date'] - $assemblerProduct['release_date'] ) <= 10000 )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['same_year_movies'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['same_year_movies'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they were released the same year score them as such and move on
                        else
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['same_year_movies'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['different_date_movies'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }
                    }
                }
                break;
            case $this->booksRootCategoryId:
                foreach( $potentialDuplicateProductsArray as $comparisonProductId => $comparisonProduct )
                {
                    if( !empty( $comparisonProduct['release_date'] ) )
                    {
                        // if the products have the same release date score it as such and move on
                        if( $comparisonProduct['release_date'] == $assemblerProduct['release_date'] )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['exact_date'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['exact_date'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they were released the same year score them as such and move on
                        elseif( abs( $comparisonProduct['release_date'] - $assemblerProduct['release_date'] ) <= 10000 )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['same_year_books'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['same_year_books'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they weren't released the same year score them as such and move on
                        else
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['different_date_books'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['different_date_books'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }
                    }
                }
                break;
            case $this->videoGamesRootCategoryId:
                foreach( $potentialDuplicateProductsArray as $comparisonProductId => $comparisonProduct )
                {
                    if( !empty( $comparisonProduct['release_date'] ) )
                    {
                        // if the products have the same release date score it as such and move on
                        if( $comparisonProduct['release_date'] == $assemblerProduct['release_date'] )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['exact_date'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['exact_date'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they were released the same year score them as such and move on
                        elseif( abs( $comparisonProduct['release_date'] - $assemblerProduct['release_date'] ) <= 100 )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['same_month_games'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['same_month_games'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they were released the same year score them as such and move on
                        elseif( abs( $comparisonProduct['release_date'] - $assemblerProduct['release_date'] ) <= 10000 )
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['same_year_games'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['same_year_games'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }

                        // if they were released the same year score them as such and move on
                        else
                        {
                            $this->debugMsg( "scoring  product : " . str_pad( $comparisonProduct['product_id'], 10, ' ' ) . "with score : " . $this->_duplicateScores['different_date_games'], 2 );
                            $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $this->_duplicateScores['different_date_games'];
                            $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += 100;
                            continue;
                        }
                    }
                }
                break;

        }
    }

    /**
     * this function gets the attributes we want to compare from the database for both the potential and assembler products
     * (it would have been in the wrong format and then discarded earlier ofr the assembler product)
     *
     * @param array $assemblerProduct
     * @param array $potentialDuplicateProductsArray
     * @param int   $assemblerProductId
     * @param array $potentialDuplicateProductIdsArray
     * @param int   $rootCategoryId
     */
    public function preparePotentialDuplicatesAttributes( array &$assemblerProduct, array &$potentialDuplicateProductsArray, $assemblerProductId, array $potentialDuplicateProductIdsArray, $rootCategoryId )
    {
        // if we need to load the product attribute type ids for the attributes we wish to load then do so
        if( !isset( $this->_attributeTypeIds['author'] ) )
        {
            $this->_attributeTypeIds['author'] = ProductAttributeType::getIdFromName( 'auhor' );
        }
        if( !isset( $this->_attributeTypeIds['publisher'] ) )
        {
            $this->_attributeTypeIds['publisher'] = ProductAttributeType::getIdFromName( 'publisher' );
        }
        if( !isset( $this->_attributeTypeIds['genre'] ) )
        {
            $this->_attributeTypeIds['genre'] = ProductAttributeType::getIdFromName( 'genre' );
        }
        if( !isset( $this->_attributeTypeIds['developer'] ) )
        {
            $this->_attributeTypeIds['developer'] = ProductAttributeType::getIdFromName( 'developer' );
        }

        switch( $rootCategoryId )
        {
            /* not doing movies atm
            case 24275:
                $attributeIds = array(
                    5,
                    8,
                );
                break;
            */
            case $this->booksRootCategoryId:
                // for books want: author, publisher, genre
                $attributeIds = array(
                    $this->_attributeTypeIds['author'],
                    $this->_attributeTypeIds['publisher'],
                    $this->_attributeTypeIds['genre'],
                );
                break;

            case $this->videoGamesRootCategoryId:
                // for games want: developer, publisher
                $attributeIds = array(
                    $this->_attributeTypeIds['publisher'],
                    $this->_attributeTypeIds['developer'],
                );
                break;

            default:
                $attributeIds = array(0);
        }

        $this->debugMsg( "getting attributes from database. Attribute Ids : " . implode( ' ', $attributeIds ) );


        // get all attributes of the right type for the products we're interested in
        $attributesSQL = "SELECT product_id, product_attribute_type_id, product_attribute_value
FROM product_attribute WHERE product_id
IN ($assemblerProductId," . implode( ',', $potentialDuplicateProductIdsArray ) . ")
AND product_attribute_type_id IN (" . implode( ',', $attributeIds ) . ");";

        $this->debugMsg( $attributesSQL, 2 );

        $attributesResult = $this->db->createCommand( $attributesSQL )
                                     ->queryAll();

        // assign the attributes to the right products
        foreach( $attributesResult as $productAttribute )
        {
            if( $productAttribute['product_id'] == $assemblerProductId )
            {
                $this->debugMsg( "adding attribute to input product" );
                // if this is the first attribute of this type
                if( empty( $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] ) )
                {
                    // if it can be json_decoded do so
                    $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] = json_decode( $productAttribute['product_attribute_value'], true ) ?: $productAttribute['product_attribute_value'];
                    if( is_array( $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] ) )
                    {
                        foreach( $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] as $subAttributeId => $subAttributeValue )
                        {
                            if( is_array( $subAttributeValue ) )
                            {
                                // if it's a 2D array put al the values into a 1D array
                                unset( $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][$subAttributeId] );
                                foreach( $subAttributeValue as $subSubAttributeValue )
                                {
                                    $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $subSubAttributeValue;
                                }
                            }
                        }
                    }
                }
                else
                {
                    // if the attribute isn't an array make it one
                    if( !is_array( $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] ) )
                    {
                        $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] = array($assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']]);
                    }
                    // try decoding the value
                    $value = json_decode( $productAttribute['product_attribute_value'], true ) ?: $productAttribute['product_attribute_value'];
                    if( is_array( $value ) )
                    {
                        // if the value is an array
                        foreach( $value as $subAttributeId => $subAttributeValue )
                        {
                            // add each subValue (or subSubValue if applicable) to the attribute array
                            if( is_array( $subAttributeValue ) )
                            {
                                // if it's a 2D array put al the values into a 1D array
                                foreach( $subAttributeValue as $subSubAttributeValue )
                                {
                                    $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $subSubAttributeValue;
                                }
                            }
                            else
                            {
                                $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $subAttributeValue;
                            }
                        }
                    }
                    else
                    {
                        $assemblerProduct['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $value;
                    }
                }
            }
            else
            {
                $this->debugMsg( "adding attribute to product id : " . $productAttribute['product_id'], 2 );
                if( empty( $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] ) )
                {
                    // if it can be json_decoded do so
                    $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] = json_decode( $productAttribute['product_attribute_value'], true ) ?: $productAttribute['product_attribute_value'];
                    if( is_array( $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] ) )
                    {
                        foreach( $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] as $subAttributeId => $subAttributeValue )
                        {
                            if( is_array( $subAttributeValue ) )
                            {
                                unset( $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][$subAttributeId] );
                                foreach( $subAttributeValue as $subSubAttributeValue )
                                {
                                    // if it's a 2D array put al the values into a 1D array (
                                    $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $subSubAttributeValue;
                                }
                            }
                        }
                    }
                }
                else
                {
                    // if the attribute isn't an array make it one
                    if( !is_array( $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] ) )
                    {
                        $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']] = array($potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']]);
                    }
                    // try decoding the value
                    $value = json_decode( $productAttribute['product_attribute_value'], true ) ?: $productAttribute['product_attribute_value'];
                    if( is_array( $value ) )
                    {
                        // if the value is an array
                        foreach( $value as $subAttributeId => $subAttributeValue )
                        {
                            // add each subValue (or subSubValue if applicable) to the attribute array
                            if( is_array( $subAttributeValue ) )
                            {
                                // if it's a 2D array put al the values into a 1D array
                                foreach( $subAttributeValue as $subSubAttributeValue )
                                {
                                    $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $subSubAttributeValue;
                                }
                            }
                            else
                            {
                                $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $subAttributeValue;
                            }
                        }
                    }
                    else
                    {
                        $potentialDuplicateProductsArray['product_id:' . $productAttribute['product_id']]['product_attributes']['product_attribute_type:' . $productAttribute['product_attribute_type_id']][] = $value;
                    }
                }
            }
        }
    }

    /**
     * This function scores potential products based on how well their attributes match with those of the assembler product
     *
     * @param array $inputProduct
     * @param array $potentialDuplicateProductsArray
     */
    public function scorePotentialDuplicatesByAttributes( array $inputProduct, array &$potentialDuplicateProductsArray )
    {
        $this->debugMsg( "scoring potential products by attributes" );
        foreach( $inputProduct['product_attributes'] as $inputProductAttributeType => $inputProductAttributeValue )
        {
            $attributeTypeId = substr( $inputProductAttributeType, 23 );
            $this->debugMsg( $attributeTypeId );
            if( is_array( $inputProductAttributeValue ) )
            {
                // if the input attribute is an array
                foreach( $potentialDuplicateProductsArray as $comparisonProductId => $comparisonProduct )
                {
                    if( !empty( $comparisonProduct['product_attributes'][$inputProductAttributeType] ) )
                    {
                        if( is_array( $comparisonProduct['product_attributes'][$inputProductAttributeType] ) )
                        {
                            // if both are arrays
                            $match = $this->matchAttributeValueArrayArray( $comparisonProduct['product_attributes'][$inputProductAttributeType], $inputProductAttributeValue, $attributeTypeId );
                        }
                        else
                        {
                            // if the input product has an array and the potential duplicate does not
                            $match = $this->matchAttributeValueArrayString( $inputProductAttributeValue, $comparisonProduct['product_attributes'][$inputProductAttributeType], $attributeTypeId );
                        }
                        $this->debugMsg( $comparisonProductId . ',  ' . $attributeTypeId . ' , ' . $match );
                        $potentialDuplicateProductsArray = $this->scorePotentialProductAttribute( $comparisonProductId, $potentialDuplicateProductsArray, $attributeTypeId, $match );
                    }
                }
            }
            else
            {
                // if the input attribute isn't an array
                foreach( $potentialDuplicateProductsArray as $comparisonProductId => $comparisonProduct )
                {
                    if( !empty( $comparisonProduct['product_attributes'][$inputProductAttributeType] ) )
                    {
                        if( is_array( $comparisonProduct['product_attributes'][$inputProductAttributeType] ) )
                        {
                            // if only the potential duplicate's attribute is an array
                            $match = $this->matchAttributeValueArrayString( $comparisonProduct['product_attributes'][$inputProductAttributeType], $inputProductAttributeValue, $attributeTypeId );
                        }
                        else
                        {
                            // if neither are arrays
                            $match = $this->matchAttributeValueStringString( $comparisonProduct['product_attributes'][$inputProductAttributeType], $inputProductAttributeValue, $attributeTypeId );
                        }
                        $this->debugMsg( $comparisonProductId . ',  ' . $attributeTypeId . ' , ' . $match );
                        $potentialDuplicateProductsArray = $this->scorePotentialProductAttribute( $comparisonProductId, $potentialDuplicateProductsArray, $attributeTypeId, $match );
                    }
                }
            }
        }
    }

    /**
     * function controls the scoring of products based on a match or not on individual attributes
     *
     * @param int   $comparisonProductId
     * @param array $potentialDuplicateProductsArray
     * @param int   $attributeTypeId
     * @param bool  $match
     *
     * @return mixed
     */
    protected function scorePotentialProductAttribute( $comparisonProductId, $potentialDuplicateProductsArray, $attributeTypeId, $match )
    {
        $score = $match ? $this->_duplicateScores['attribute_match'][$attributeTypeId] : $this->_duplicateScores['attribute_mismatch'][$attributeTypeId];
        $this->debugMsg( "scoring  product : " . str_pad( $comparisonProductId, 10, ' ' ) . "with score : $score", 2 );
        //if they are different score them as not matching
        $potentialDuplicateProductsArray[$comparisonProductId]['duplicate_score'] += $score;
        $potentialDuplicateProductsArray[$comparisonProductId]['max_score'] += $this->_duplicateScores['attribute_match'][$attributeTypeId];

        return $potentialDuplicateProductsArray;
    }

    /**
     * function attempts to at least partially match two attribute values that are arrays
     *
     * @param array $firstAttributeValueArray
     * @param array $secondAttributeValueArray
     * @param int   $attributeTypeId
     *
     * @return bool
     */
    public function matchAttributeValueArrayArray( array $firstAttributeValueArray, array $secondAttributeValueArray, $attributeTypeId )
    {
        foreach( $firstAttributeValueArray as $firstAttributeValue )
        {
            // call array/string on each value, return true if even one if valid
            if( $this->matchAttributeValueArrayString( $secondAttributeValueArray, $firstAttributeValue, $attributeTypeId ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * function attempts to match a string against any element in an array
     *
     * @param array  $firstAttributeValueArray
     * @param string $secondAttributeValue
     * @param int    $attributeTypeId
     *
     * @return bool
     */
    public function matchAttributeValueArrayString( array $firstAttributeValueArray, $secondAttributeValue, $attributeTypeId )
    {
        foreach( $firstAttributeValueArray as $firstAttributeValue )
        {
            // call string/string on each value, return true if even one is valid
            if( $this->matchAttributeValueStringString( $firstAttributeValue, $secondAttributeValue, $attributeTypeId ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * function attempts to match two attribute values that are both strings
     *
     * @param string $firstAttributeValue
     * @param string $secondAttributeValue
     * @param int    $attributeTypeId
     *
     * @return bool
     */
    public function matchAttributeValueStringString( $firstAttributeValue, $secondAttributeValue, $attributeTypeId )
    {
        // if we need to load the product attribute type ids for the attributes we wish to load then do so
        if( !isset( $this->_attributeTypeIds['author'] ) )
        {
            $this->_attributeTypeIds['author'] = ProductAttributeType::getIdFromName( 'auhor' );
        }

        switch( $attributeTypeId )
        {
            case $this->_attributeTypeIds['author']: // author
                if( $firstAttributeValue == $secondAttributeValue )
                {
                    // if it's an exact match
                    return true;
                }
                elseif( ( strpos( strtolower( $firstAttributeValue ), strtolower( $secondAttributeValue ) ) !== false ) or ( strpos( strtolower( $secondAttributeValue ), strtolower( $firstAttributeValue ) ) !== false ) )
                {
                    // if it's a partial match
                    return true;
                }
                else
                {
                    // check whether one of the values is a name in initials e.g. J. R. R. Tolkien vs John Robert R???? Tolkien

                    //remove .
                    $firstAttributeValue = str_replace( '.', ' ', $firstAttributeValue );
                    $secondAttributeValue = str_replace( '.', ' ', $secondAttributeValue );
                    // split both names into words
                    $explodedFirstValue = explode( ' ', strtolower( trim( preg_replace( '/\s+/', ' ', $firstAttributeValue ) ) ) );
                    $explodedSecondValue = explode( ' ', strtolower( trim( preg_replace( '/\s+/', ' ', $secondAttributeValue ) ) ) );


                    foreach( $explodedFirstValue as $key => $word )
                    {
                        // check each word. if its an initial (A.) and the corresponding word in the other name starts with the same letter then update them both to be that letter
                        if( strlen( $word ) === 1 && isset( $explodedSecondValue[$key] ) && $word[0] === $explodedSecondValue[$key][0] )
                        {
                            $explodedFirstValue[$key] = $word[0];
                            $explodedSecondValue[$key] = $word[0];
                        }
                    }
                    foreach( $explodedSecondValue as $key => $word )
                    {
                        // check each word. if its an initial (A.) and the corresponding word in the other name starts with the same letter then update them both to be that letter
                        if( strlen( $word ) === 1 && isset( $explodedFirstValue[$key] ) && $word[0] === $explodedFirstValue[$key][0] )
                        {
                            $explodedFirstValue[$key] = $word[0];
                            $explodedSecondValue[$key] = $word[0];
                        }
                    }
                    if( $explodedFirstValue == $explodedSecondValue )
                    {
                        // if it now matches
                        return true;
                    }
                }
                break;
            default: // anything else
                if( $firstAttributeValue == $secondAttributeValue )
                {
                    // if it's an exact match
                    return true;
                }
                elseif( ( strpos( $firstAttributeValue, $secondAttributeValue ) !== false ) or ( strpos( $secondAttributeValue, $firstAttributeValue ) !== false ) )
                {
                    // if it's a partial match
                    return true;
                }
        }

        return false;
    }

    /**
     * This function attempts to find duplicates that have similar ratings given.
     *
     * @param array $potentialDuplicateProductsArray
     * @param array $potentialDuplicateIdsArray
     * @param array $duplicateProductIdsArray
     * @param int   $assemblerProductId
     */
    public function findPotentialDuplicatesBySimilarRatings( array &$potentialDuplicateProductsArray, array &$potentialDuplicateIdsArray, array &$duplicateProductIdsArray, $assemblerProductId )
    {
        $this->debugMsg( "Finding duplicates by similar ratings" );
        // get the number of users who have rated both products and the  number who rated both products the same
        $ratingsSQL = "SELECT ura2.product_id, COUNT(IF( ura1.rating = ura2.rating,1,NULL)) AS same_ratings, COUNT(1) AS common_users FROM user_rating ura1 JOIN user_rating ura2 ON ura1.user_id=ura2.user_id
WHERE ura1.product_id = $assemblerProductId AND ura2.product_id IN (" . implode( ',', $potentialDuplicateIdsArray ) . ") GROUP BY ura2.product_id;";

        $this->debugMsg( $ratingsSQL, 2 );

        $ratingsResult = $this->db->createCommand( $ratingsSQL )
                                  ->queryAll();

        foreach( $ratingsResult as $ratingResult )
        {
            if( $ratingResult['common_users'] >= 50 )
            {
                if( $ratingResult['same_ratings'] / $ratingResult['common_users'] >= 0.97 )
                {
                    $this->debugMsg( "found duplicate Id : " . $ratingResult['product_id'], 2 );
                    // if there is a lot of overlap and the overwhelming majority are the same then take it to be a duplicate.
                    $duplicateProductIdsArray[] = $ratingResult['product_id'];
                    unset( $potentialDuplicateProductsArray['product_id:' . $ratingResult['product_id']] );
                    unset( $potentialDuplicateIdsArray[$ratingResult['product_id']] );
                }
            }
        }

    }

    /**
     * this function attempts to find duplicate products by virtue if them having identical reviews
     *
     * @param array $potentialDuplicateProductsArray
     * @param array $potentialDuplicateIdsArray
     * @param array $duplicateProductIdsArray
     * @param int   $assemblerProductId
     */
    public function findPotentialDuplicatesBySimilarReviews( array &$potentialDuplicateProductsArray, array &$potentialDuplicateIdsArray, array &$duplicateProductIdsArray, $assemblerProductId )
    {
        $this->debugMsg( "Finding duplicates by similar reviews" );
        // get the number of reviews in common and te number of reviews that are the same for both products
        $reviewsSQL = "SELECT ure2.product_id, COUNT(IF( ure1.review_note = ure2.review_note,1,NULL)) AS same_reviews, COUNT(1) AS common_users FROM user_review ure1 JOIN user_review ure2 ON ure1.user_id=ure2.user_id
WHERE ure1.product_id = $assemblerProductId AND ure2.product_id IN (" . implode( ',', $potentialDuplicateIdsArray ) . ") GROUP BY ure2.product_id;";

        $reviewsResult = $this->db->createCommand( $reviewsSQL )
                                  ->queryAll();

        foreach( $reviewsResult as $reviewResult )
        {
            if( $reviewResult['common_users'] >= 10 )
            {
                if( $reviewResult['same_reviews'] / $reviewResult['common_users'] >= 0.9 )
                {
                    $this->debugMsg( "found duplicate Id : " . $reviewResult['product_id'], 2 );
                    // if there are several users with reviews in common and the majority are identical then mark it as a duplicate
                    $duplicateProductIdsArray[] = $reviewResult['product_id'];
                    unset( $potentialDuplicateProductsArray['product_id:' . $reviewResult['product_id']], $potentialDuplicateIdsArray[$reviewResult['product_id']] );
                }
            }
        }
    }

    /**
     * this function attempts to find duplicates by virtue of identical product descriptions.
     *
     * @param array $potentialDuplicateProductsArray
     * @param array $potentialDuplicateIdsArray
     * @param array $duplicateProductIdsArray
     * @param array $assemblerProduct
     */
    public function findPotentialDuplicatesBySimilarDescriptions( array &$potentialDuplicateProductsArray, array &$potentialDuplicateIdsArray, array &$duplicateProductIdsArray, array $assemblerProduct )
    {
        $this->debugMsg( "Finding duplicates by similar descriptions" );
        $assemblerProductDescriptionComparison = substr( preg_replace( "/[^a-z]+/", ' ', strtolower( strip_tags( $assemblerProduct['product_description'] ) ) ), 0, 100 );
        if( strlen( $assemblerProductDescriptionComparison ) == 100 )
        {
            foreach( $potentialDuplicateProductsArray as $key => $potentialDuplicateProduct )
            {
                $potentialDuplicateProductDescriptionComparison = substr( preg_replace( "/[^a-z]+/", ' ', strtolower( strip_tags( $potentialDuplicateProduct['product_description'] ) ) ), 0, 100 );
                // if both the product and the assemblerProduct have at least 100 characters and the 1sy 100 characters are the same mark it as a duplicate
                if( strlen( $potentialDuplicateProductDescriptionComparison ) == 100 && $assemblerProductDescriptionComparison == $potentialDuplicateProductDescriptionComparison )
                {
                    $this->debugMsg( "found duplicate Id : " . $potentialDuplicateProduct['product_id'], 2 );
                    $duplicateProductIdsArray[] = $potentialDuplicateProduct['product_id'];
                    unset( $potentialDuplicateProductsArray['product_id:' . $potentialDuplicateProduct['product_id']], $potentialDuplicateIdsArray[$potentialDuplicateProduct['product_id']] );
                }
            }
        }
    }

    /**
     * This function selects the product_ids of those products that have scores equal to or higher than a certain threshold
     *
     * @param array $potentialDuplicateProductsArray
     * @param array $potentialDuplicateProductIdsArray
     *
     * @return array
     */
    public function decideDuplicateProducts( array &$potentialDuplicateProductsArray, array &$potentialDuplicateProductIdsArray )
    {
        $this->debugMsg( "deciding duplicates by score" );
        $duplicateProductIdsArray = array();

        foreach( $potentialDuplicateProductsArray as $key => $potentialDuplicateProduct )
        {
            $this->debugMsg( $potentialDuplicateProduct['duplicate_score'] . ' >= ' . ($potentialDuplicateProduct['max_score'] * $this->_thresholdPercentScore ));
            if( $potentialDuplicateProduct['duplicate_score'] >= $potentialDuplicateProduct['max_score'] * $this->_thresholdPercentScore )
            {
                $this->debugMsg( "found duplicate Id : " . $potentialDuplicateProduct['product_id'], 2 );
                $duplicateProductIdsArray[] = $potentialDuplicateProduct['product_id'];
                unset( $potentialDuplicateProductsArray[$key], $potentialDuplicateProductIdsArray[$potentialDuplicateProduct['product_id']] );
            }
        }

        return $duplicateProductIdsArray;
    }

    /**
     * function resolves all conflicts over a given product (either now or queued)
     *
     * @param int  $conflictedProductId
     * @param bool $queueOnly
     *
     * @return bool
     */
    public function resolveConflictByConflictedProductId( $conflictedProductId, $queueOnly = false )
    {
        /**
         * @var DataProviderProduct $conflictedProductDuplicationDataProviderProduct
         */
        $relatedProducts = $this->getProductRelations( $conflictedProductId );
        if( $relatedProducts[self::CONFLICTED_MASTER_RELATION_ID] && !empty( $relatedProducts[self::CONFLICTED_MASTER_RELATION_ID] ) )
        {
            $conflictedProductDuplicates = $relatedProducts[self::CONFLICTED_RELATION_ID];
            $conflictedMastersCount = $relatedProducts[self::CONFLICTED_MASTER_RELATION_ID];
            for( $i = 0; $i <= $conflictedMastersCount - 2; $i += 1 )
            {
                for( $j = $i + 1; $j <= $conflictedMastersCount - 1; $j += 1 )
                {
                    // resolve all the conflicts between the masters
                    $this->resolveConflictByMasterIds( $conflictedProductDuplicates[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY][$i], $conflictedProductDuplicates[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY][$j], $queueOnly );
                }
            }
        }

        // if we can't find a data provider product or if it isn't set as a conflicted product then exit
        return true;
    }

    /**
     * function resolves the conflict between two masters (could be queued)
     *
     * @param int  $masterAlphaProductId
     * @param int  $masterBetaProductId
     * @param bool $queueOnly
     *
     * @return bool
     */
    public function resolveConflictByMasterIds( $masterAlphaProductId, $masterBetaProductId, $queueOnly = false )
    {
        // build params
        $params = array(
            'master_alpha_product_id' => $masterAlphaProductId,
            'master_beta_product_id'  => $masterBetaProductId
        );

        // run or queue the conflict resolution
        return $this->resolveConflictByMasterIdsQueued( $params, $queueOnly );
    }

    /**
     * function actually does the conflict resolution between two masters
     *
     * @param array $params
     * @param bool  $queueOnly
     *
     * @return bool
     */
    public function resolveConflictByMasterIdsQueued( array $params, $queueOnly = false )
    {
        if( $queueOnly )
        {
            // queue the resolution for later
            $this->queueDataUpdate( $this->dataProviderId, __FUNCTION__, $params, self::MEDIUM_PRIORITY_QUEUE_ID );

            return true;
        }
        else
        {
            // extract the necessary info from the params
            $masterAlphaProductId = $params['master_alpha_product_id'];
            $masterBetaProductId = $params['master_beta_product_id'];
            $validMasters = true;

            /**
             * @var DataProviderProduct $alphaProductDuplicationDataProviderProduct
             */
            // check the alpha product is a valid master
            if( !$this->checkIsMaster( $masterAlphaProductId ) )
            {
                // if it's no longer a master don't do the resolution
                $validMasters = false;
            }

            /**
             * @var DataProviderProduct $betaProductDuplicationDataProviderProduct
             */
            // check the beta product is a valid master
            if( !$this->checkIsMaster( $masterBetaProductId ) )
            {
                // if it's no longer a master don't do the resolution
                $validMasters = false;
            }

            if( $validMasters )
            {
                $this->_productAssemblerService->setDebug( $this->debug );
                // assemble the alpha master (do it now, don't queue duplication, don't force it if it's up to date)
                $this->_productAssemblerService->assembleProductByProductIdQueued( array(
                    'product_id'           => $masterAlphaProductId,
                    'no_duplication_queue' => true
                ), false );

                // assemble the beta master (do it now, don't queue duplication, don't force it if it's up to date)
                $this->_productAssemblerService->assembleProductByProductIdQueued( array(
                    'product_id'           => $masterBetaProductId,
                    'no_duplication_queue' => true
                ), false );

                // force duplicate detection on the alpha master (don't queue any more)
                $this->updateProduct( $masterAlphaProductId, false, true, false, false );

                // force duplicate detection on the beta master (don't queue any more)
                $this->updateProduct( $masterBetaProductId, false, true, false, false );

                $alphaProductRelations = $this->getProductRelations( $masterAlphaProductId );
                $betaProductRelations = $this->getProductRelations( $masterBetaProductId );

                // if alpha has duplicate masters and beta is one or if beta has duplicate masters and alpha is one (either both or neither should be true but...)
                if( ( !empty( $alphaProductRelations[self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY] ) && in_array( $masterBetaProductId, $alphaDuplicateProductIdsArray[self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY] ) ) or ( !empty( $betaProductRelations[self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY] ) && in_array( $masterAlphaProductId, $betaProductRelations[self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY] ) ) )
                {
                    // consolidate the masters into a single product
                    $this->consolidateMasterProducts( $masterAlphaProductId, $masterBetaProductId );
                    $this->_productAssemblerService->assembleProductByProductId( $masterAlphaProductId, true );

                    return true;
                }

                // Array to store PDDPPs (to avoid looking them up too often)
                /**
                 * @var DataProviderProduct[] $productDuplicationDataProviderProductsArray
                 */
                $productDuplicationDataProviderProductsArray = array();

                // get an array of subordinate product ids of alpha
                $alphaSubordinateProductIdsArray = $alphaProductRelations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];

                foreach( $alphaSubordinateProductIdsArray as $key => $alphaSubordinateProductId )
                {
                    // make sure we've loaded the PDDPP
                    if( !isset( $productDuplicationDataProviderProductsArray[$alphaSubordinateProductId] ) )
                    {
                        $productDuplicationDataProviderProductsArray[$alphaSubordinateProductId] = ProductRelation::model()
                                                                                                                  ->findByAttributes( array(
                                                                                                                      'related_product_id' => $alphaSubordinateProductId,
                                                                                                                      'product_id'         => $masterAlphaProductId
                                                                                                                  ) );
                    }
                    // check each subordinate has the relationship correctly, remove those that don't
                    if( !$this->checkDuplicateRelationship( $masterAlphaProductId, $alphaSubordinateProductId, self::SUBORDINATE_MASTER_RELATIONSHIP ) )
                    {
                        unset( $alphaSubordinateProductIdsArray[$key] );
                    }
                }
                // and a count of them
                $alphaSubordinatesCount = count( $alphaSubordinateProductIdsArray );

                // get an array of subordinate product ids of beta
                $betaSubordinateProductIdsArray = $betaProductRelations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY];
                foreach( $betaSubordinateProductIdsArray as $key => $betaSubordinateProductId )
                {
                    // make sure we've loaded the PDDPP
                    if( !isset( $productDuplicationDataProviderProductsArray[$betaSubordinateProductId] ) )
                    {
                        $productDuplicationDataProviderProductsArray[$betaSubordinateProductId] = ProductRelation::model()
                                                                                                                 ->findByAttributes( array(
                                                                                                                     'related_product_id' => $betaSubordinateProductId,
                                                                                                                     'product_id'         => $masterBetaProductId
                                                                                                                 ) );
                    }
                    // check each subordinate has the relationship correctly, remove those that don't
                    if( !$this->checkDuplicateRelationship( $masterBetaProductId, $betaSubordinateProductId, self::SUBORDINATE_MASTER_RELATIONSHIP ) )
                    {
                        unset( $betaSubordinateProductIdsArray[$key] );
                    }
                }
                // and a count
                $betaSubordinatesCount = count( $betaSubordinateProductIdsArray );

                // the overlap between the two are conflicted between them
                $conflictedProductIdsArray = array_intersect_key( $alphaSubordinateProductIdsArray, $betaSubordinateProductIdsArray );
                $conflictedCount = count( $conflictedProductIdsArray );

                // if the total number of products that are currently conflicted between the two is >=50% then the two masters are duplicates (maybe change to 33% or 40%?)
                if( ( ( $conflictedCount * 2 ) >= $alphaSubordinatesCount ) || ( ( $conflictedCount * 2 ) >= $betaSubordinatesCount ) )
                {
                    // consolidate the masters into a single product
                    $this->consolidateMasterProducts( $masterAlphaProductId, $masterBetaProductId );

                    // queue that product to the assembler (with force update set)
                    $this->_productAssemblerService->assembleProductByProductId( $masterAlphaProductId, true );

                    return true;
                }

                // get the products that are subordinates of alpha and have been automatically set as not duplicates
                $alphaSubordinateBetaResolvedProductIdsArray = array_intersect( $alphaSubordinateProductIdsArray, $betaDuplicateProductIdsArray[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] );
                foreach( $alphaSubordinateBetaResolvedProductIdsArray as $key => $betaNotDuplicateProductId )
                {
                    // check each product has the relationship correctly, remove those that don't
                    if( !$this->checkDuplicateRelationship( $betaProductDuplicationDataProviderProduct, $productDuplicationDataProviderProductsArray[$betaNotDuplicateProductId], self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP ) )
                    {
                        unset( $alphaSubordinateBetaResolvedProductIdsArray[$key] );
                    }
                }
                // and a count
                $alphaSubordinateBetaResolvedCount = count( $alphaSubordinateBetaResolvedProductIdsArray );

                $betaSubordinateAlphaResolvedProductIdsArray = array_intersect( $betaSubordinateProductIdsArray, $alphaDuplicateProductIdsArray[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] );
                foreach( $betaSubordinateAlphaResolvedProductIdsArray as $key => $alphaNotDuplicateProductId )
                {
                    // check each product has the relationship correctly, remove those that don't
                    if( !$this->checkDuplicateRelationship( $alphaProductDuplicationDataProviderProduct, $productDuplicationDataProviderProductsArray[$alphaNotDuplicateProductId], self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP ) )
                    {
                        unset( $betaSubordinateAlphaResolvedProductIdsArray[$key] );
                    }
                }
                // and a count
                $betaSubordinateAlphaResolvedCount = count( $betaSubordinateAlphaResolvedProductIdsArray );

                // if the total number of products that have ever been conflicted between the two masters is >=66% then the two masters are duplicates (maybe change t >50%?)
                if( ( ( ( $conflictedCount + $alphaSubordinateBetaResolvedCount + $betaSubordinateAlphaResolvedCount ) * 1.5 ) >= ( $alphaSubordinatesCount + $betaSubordinateAlphaResolvedCount ) ) || ( ( ( $conflictedCount + $betaSubordinateAlphaResolvedCount + $alphaSubordinateBetaResolvedCount ) * 1.5 ) >= ( $betaSubordinatesCount + $alphaSubordinateBetaResolvedCount ) ) )
                {
                    // consolidate the masters into a single product
                    $this->consolidateMasterProducts( $masterAlphaProductId, $masterBetaProductId );

                    // queue that product to the assembler (with force update set)
                    $this->_productAssemblerService->assembleProductByProductId( $masterAlphaProductId, true );

                    return true;
                }

                /** @var  $conflictedProductId */
                // at this point the master's can't really be called duplicates of each other so we're going to have to decide between each master for each conflicted product
                foreach( $conflictedProductIdsArray as $conflictedProductId )
                {
                    $this->decideBetweenMastersForSubordinate( $productDuplicationDataProviderProductsArray[$conflictedProductId], $alphaProductDuplicationDataProviderProduct, $betaProductDuplicationDataProviderProduct );
                }
                // queue both masters to be updated by the assembler (with force update set)
                $this->_productAssemblerService->assembleProductByProductId( $masterAlphaProductId, true );
                $this->_productAssemblerService->assembleProductByProductId( $masterBetaProductId, true );
            }
            else
            {
                return false;
            }

            return true;
        }
    }

    /**
     * This function adds all of one master product's subordinates to another master then deletes the spare.
     * it also copies previous conflict resolutions to the target master
     *
     * @param int $alphaProductId
     * @param int $betaProductId
     */
    private function consolidateMasterProducts( $alphaProductId, $betaProductId )
    {
        // get the lists of duplicate products
        $betaDuplicateProductIdsArray = $this->getProductRelations( $betaProductId );

        // get an array of subordinate product ids of beta
        foreach( $betaDuplicateProductIdsArray[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] as $betaSubordinateProductId )
        {
            /**
             * @var DataProviderProduct $temporaryProductDuplicationDataProviderProduct
             */
            // get the PDDPP for each subordinate then check it has the relationship correctly
            if( ( $temporaryProductDuplicationDataProviderProduct = ProductRelation::model()
                                                                                   ->findByAttributes( array(
                                                                                       'related_product_id' => $betaSubordinateProductId,
                                                                                       'product_id'         => $betaProductId
                                                                                   ) ) ) && $this->checkDuplicateRelationship( $betaSubordinateProductId, $betaProductId, self::SUBORDINATE_MASTER_RELATIONSHIP )
            )
            {
                // add a subordinate master relationship between that product and alpha (do overwrite any existing relationship)
                $this->createDuplicateRelationship( $betaSubordinateProductId, $alphaProductId, self::SUBORDINATE_MASTER_RELATIONSHIP, true );
                // delete the relationship between the product and beta
                $this->deleteDuplicateRelationship( $betaSubordinateProductId, $betaProductId, self::SUBORDINATE_MASTER_RELATIONSHIP );
            }
        }

        if( !empty( $betaDuplicateProductIdsArray[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] ) )
        {
            //for each products that has had conflict resolutions against beta
            foreach( $betaDuplicateProductIdsArray[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] as $key => $betaResolvedAgainstProductId )
            {
                /**
                 * @var DataProviderProduct $temporaryProductDuplicationDataProviderProduct
                 */
                // get the PDDPP then check the relationship
                if( ( $temporaryProductDuplicationDataProviderProduct = ProductRelation::model()
                                                                                       ->findByAttributes( array(
                                                                                           'related_product_id' => $betaSubordinateProductId,
                                                                                           'product_id'         => $betaProductId
                                                                                       ) ) ) && $this->checkDuplicateRelationship( $betaSubordinateProductId, $betaProductId, self::SUBORDINATE_MASTER_RELATIONSHIP )
                )
                {
                    // add a resolved conflict between that product and alpha (don't overwrite any existing relationship)
                    $this->createDuplicateRelationship( $betaSubordinateProductId, $alphaProductId, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP );
                    // delete the resolution between the product and beta
                    $this->deleteDuplicateRelationship( $betaSubordinateProductId, $betaProductId, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP );
                }
            }
        }
        Utilities::deleteProduct( $betaProductId->product_id );

        $this->messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $betaProductId->product_id, true );
    }

    /**
     * @param DataProviderProduct|int $subordinateInput
     * @param DataProviderProduct|int $masterAlphaInput
     * @param DataProviderProduct|int $masterBetaInput
     *
     * @todo test
     * @return null
     */
    protected function decideBetweenMastersForSubordinate( $subordinateInput, $masterAlphaInput, $masterBetaInput )
    {
        // check the subordinate input
        $subordinateProductDuplicationDataProviderProduct = &$this->getProductRelations( $subordinateInput, 'first' );
        if( is_null( $subordinateProductDuplicationDataProviderProduct ) or !isset( $subordinateProductDuplicationDataProviderProduct[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] ) )
        {
            // if the subordinate product is not a conflicted subordinate bail
            return null;
        }

        // check master alpha input
        $masterAlphaProductDuplicationDataProviderProduct = &$this->getProductRelations( $masterAlphaInput, 'second' );
        if( is_null( $masterAlphaProductDuplicationDataProviderProduct ) or !isset( $masterAlphaProductDuplicationDataProviderProduct[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] ) )
        {
            // if the subordinate and master are not in the correct relationship then bail
            return null;
        }

        // check master beta input
        $masterBetaProductDuplicationDataProviderProduct = &$this->getProductRelations( $masterBetaInput, 'third' );
        if( is_null( $masterBetaProductDuplicationDataProviderProduct ) or !$this->checkDuplicateRelationship( $subordinateProductDuplicationDataProviderProduct, $masterBetaInput, self::SUBORDINATE_MASTER_RELATIONSHIP ) )
        {
            // if the subordinate and master are not in the correct relationship then bail
            return null;
        }

        // sql to get details we want from the db
        $comparisonDetailsSQL = "SELECT product_id, product_name, release_date, product_description FROM product WHERE product_id IN (?,?,?);";

        //do query
        $comparisonDetailsResults = $this->db->createCommand( $comparisonDetailsSQL )
                                             ->queryAll( array(
                                                 $subordinateProductDuplicationDataProviderProduct->product_id,
                                                 $masterAlphaProductDuplicationDataProviderProduct->product_id,
                                                 $masterBetaProductDuplicationDataProviderProduct->product_id
                                             ) );
        // arrays to hold details
        $subordinateProductDetailsArray = array();
        $masterAlphaProductDetailsArray = array();
        $masterBetaProductDetailsArray = array();

        foreach( $comparisonDetailsResults as $comparisonProductDetails )
        {
            // extract details from query result
            if( $comparisonDetailsResults['product_id'] == $subordinateProductDuplicationDataProviderProduct->product_id )
            {
                $subordinateProductDetailsArray = array(
                    'product_name'        => $comparisonProductDetails['product_name'],
                    'product_description' => $comparisonProductDetails['product_description'],
                    'release_date'        => $comparisonProductDetails['release_date'],
                );
            }
            elseif( $comparisonDetailsResults['product_id'] == $masterAlphaProductDuplicationDataProviderProduct->product_id )
            {
                $masterAlphaProductDetailsArray = array(
                    'product_name'        => $comparisonProductDetails['product_name'],
                    'product_description' => $comparisonProductDetails['product_description'],
                    'release_date'        => $comparisonProductDetails['release_date'],
                );
            }
            elseif( $comparisonDetailsResults['product_id'] == $masterBetaProductDuplicationDataProviderProduct->product_id )
            {
                $masterBetaProductDetailsArray = array(
                    'product_name'        => $comparisonProductDetails['product_name'],
                    'product_description' => $comparisonProductDetails['product_description'],
                    'release_date'        => $comparisonProductDetails['release_date'],
                );
            }
        }

        // if the name exactly matches alpha and not beta
        if( $subordinateProductDetailsArray['product_name'] == $masterAlphaProductDetailsArray['product_name'] && $subordinateProductDetailsArray['product_name'] != $masterBetaProductDetailsArray['product_name'] )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        // if the name exactly matches beta and not alpha
        if( $subordinateProductDetailsArray['product_name'] == $masterBetaProductDetailsArray['product_name'] && $subordinateProductDetailsArray['product_name'] != $masterAlphaProductDetailsArray['product_name'] )
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }

        // if the release_date exactly matches alpha and not beta
        if( $subordinateProductDetailsArray['release_date'] == $masterAlphaProductDetailsArray['release_date'] && $subordinateProductDetailsArray['release_date'] != $masterBetaProductDetailsArray['release_date'] )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        // if the release_date exactly matches beta and not alpha
        if( $subordinateProductDetailsArray['release_date'] == $masterBetaProductDetailsArray['release_date'] && $subordinateProductDetailsArray['release_date'] != $masterAlphaProductDetailsArray['release_date'] )
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }

        // if the product_description exactly matches alpha and not beta
        if( $subordinateProductDetailsArray['product_description'] == $masterAlphaProductDetailsArray['product_description'] && $subordinateProductDetailsArray['product_description'] != $masterBetaProductDetailsArray['product_description'] )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        // if the product_description exactly matches beta and not alpha
        if( $subordinateProductDetailsArray['product_description'] == $masterBetaProductDetailsArray['product_description'] && $subordinateProductDetailsArray['product_description'] != $masterAlphaProductDetailsArray['product_description'] )
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }

        // SQL query to get numbers of exact review matches with the master products
        $reviewComparisonSQL = "SELECT ure2.product_id, COUNT(1) AS common_review_count FROM user_review ure1 JOIN user_review ure2 ON ure1.user_id = ure2.user_id AND ure1.review_note = ure2.review_note
                                WHERE ure1.product_id = " . $subordinateInput->product_id . "
                                AND ure2.product_id IN (" . $masterAlphaInput . "," . $masterBetaInput . ")
                                 GROUP BY ure2.product_id;";

        // query execution and data extraction
        $reviewComparisonResult = $this->db->createCommand( $reviewComparisonSQL )
                                           ->queryAll();
        $reviewComparisonArray = array_column( $reviewComparisonResult, 'common_review_count', 'product_id' );

        // if there are reviews in common with alpha and either there are no reviews in common with beta or there are 25% more reviews in common with alpha than beta
        if( !empty( $reviewComparisonArray[$masterAlphaInput] ) && ( empty( $reviewComparisonArray[$masterBetaInput] ) or $reviewComparisonArray[$masterAlphaInput] >= $reviewComparisonArray[$masterBetaInput] * 1.25 ) )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        // if there are reviews in common with beta and either there are no reviews in common with alpha or there are 25% more reviews in common with beta than alpha
        if( !empty( $reviewComparisonArray[$masterBetaInput] ) && ( empty( $reviewComparisonArray[$masterAlphaInput] ) or $reviewComparisonArray[$masterBetaInput] >= $reviewComparisonArray[$masterAlphaInput] * 1.25 ) )
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }

        // SQL query to find the number of common categories
        $categoryComparisonSQL = "SELECT cp2.product_id, COUNT(1) AS common_category_count FROM category_product cp1 JOIN category_product cp2 ON cp1.category_id = cp2.category_id
WHERE cp1.product_id = " . $subordinateInput . "
AND cp2.product_id IN (" . $masterAlphaInput . "," . $masterBetaInput . ") GROUP BY cp2.product_id;";

        // execute query and extract data
        $categoryComparisonResult = $this->db->createCommand( $categoryComparisonSQL )
                                             ->queryAll();
        $categoryComparisonArray = array_column( $categoryComparisonResult, 'common_category_count', 'product_id' );

        // if there are categories in common with alpha and either there are no categories in common with beta or there are more categories in common with alpha than beta
        if( !empty( $categoryComparisonArray[$masterAlphaInput] ) && ( empty( $categoryComparisonArray[$masterBetaInput] ) or $categoryComparisonArray[$masterAlphaInput] > $categoryComparisonArray[$masterBetaInput] * 1.25 ) )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        // if there are categories in common with beta and either there are no categories in common with alpha or there are more categories in common with beta than alpha
        if( !empty( $categoryComparisonArray[$masterBetaInput] ) && ( empty( $categoryComparisonArray[$masterAlphaInput] ) or $categoryComparisonArray[$masterBetaInput] > $categoryComparisonArray[$masterAlphaInput] * 1.25 ) )
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }

        // SQL query to find numbers of common related items
        $relatedProductComparisonSQL = "SELECT rp2.product_id, COUNT(1) AS common_related_product_count FROM related_product rp1 JOIN related_product rp2 ON rp1.related_product_id = rp2.related_product_id
WHERE rp1.product_id = " . $subordinateInput . "
AND rp2.product_id IN (" . $masterAlphaInput . "," . $masterBetaInput . ") GROUP BY rp2.product_id;";

        // execute query and extract data
        $relatedProductComparisonResult = $this->db->createCommand( $relatedProductComparisonSQL )
                                                   ->queryAll();
        $relatedProductComparisonArray = array_column( $relatedProductComparisonResult, 'common_related_product_count', 'product_id' );

        // if there are reviews in common with alpha and either there are no reviews in common with beta or there are 25% more reviews in common with alpha than beta
        if( !empty( $relatedProductComparisonArray[$masterAlphaInput] ) && ( empty( $relatedProductComparisonArray[$masterBetaInput] ) or $relatedProductComparisonArray[$masterAlphaInput] >= $relatedProductComparisonArray[$masterBetaInput] * 1.25 ) )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        // if there are reviews in common with beta and either there are no reviews in common with alpha or there are 25% more reviews in common with beta than alpha
        if( !empty( $relatedProductComparisonArray[$masterBetaInput] ) && ( empty( $relatedProductComparisonArray[$masterAlphaInput] ) or $relatedProductComparisonArray[$masterBetaInput] >= $relatedProductComparisonArray[$masterAlphaInput] * 1.25 ) )
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }

        // we've failed to discern which of the masters is a better fit with the product, so just go with the newest one (highest product_id)
        if( $masterAlphaInput > $masterBetaInput )
        {
            // overwrite its subordinate relationship with beta with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterBetaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
        else
        {
            // overwrite its subordinate relationship with alpha with a resolved as not duplicate relationship then end
            $this->createDuplicateRelationship( $subordinateInput, $masterAlphaInput, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP, true );

            return;
        }
    }

    /**
     * Function validates the relationship between two products given either their product id
     *
     * @param int        $inputAlpha
     * @param int        $inputBeta
     * @param int|string $relationshipType
     *
     * @return bool|null
     */
    public function checkDuplicateRelationship( $alphaProductId, $betaProductId, $relationshipType = null )
    {
        //if relationshipType is and id lets get a string
        if( is_numeric( $relationshipType ) )
        {
            $relationshipType = $this->getRelationshipKeyTypeFromId( $relationshipType );
        }
        elseif( is_string( $relationshipType ) )
        {
            $relationshipType = $this->getRelationshipKeyTypeFromString( $relationshipType );
        }
        // get the relationship array for from master to subordinate
        $alphaProductRelationships = $this->getProductRelations( $alphaProductId );

        if( empty( $alphaProductRelationships ) )
        {
            return false;
        }
        else
        {
            if( $relationshipType != null && !empty( $alphaProductRelationships[$relationshipType] ) )
            {
                if( in_array( $betaProductId, $alphaProductRelationships[$relationshipType] ) )
                {
                    $this->debugMsg( $relationshipType . " relationship exists", 2 );

                    return true;
                }
            }
            else
            {
                //if the re is a relationship return true
                foreach( $alphaProductRelationships as $relationship )
                {
                    if( in_array( $betaProductId, $relationship ) )
                    {
                        $this->debugMsg( "\tRelationship exists", 2 );

                        return true;
                    }
                }
            }
        }

        //if not return false
        $this->debugMsg( "\tRelationship does not exist", 2 );

        return false;

    }

    /**
     * function creates a given relationship between two products (They must have the correct type of PDDPPs) (flag to create new (Unique) ones)
     *
     * @param int    $inputAlpha
     * @param int    $inputBeta
     * @param string $relationship
     * @param bool   $overwritePreviousRelationship
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function createDuplicateRelationship( $alphaProductId, $betaProductId, $relationship, $overwritePreviousRelationship = false )
    {

        $this->debugMsg( "Creating relationship: $relationship :between: $alphaProductId and $betaProductId" );

        // get the relationship array for both
        $alphaProductRelationships = $this->getProductRelations( $alphaProductId );
        $betaProductRelationships = $this->getProductRelations( $betaProductId );
        if( is_numeric( $relationship ) )
        {
            $relationship = $this->getRelationshipTypeFromId( $relationship );
        }
        elseif( is_string( $relationship ) )
        {
            $relationship = $this->getRelationshipKeyTypeFromString( $relationship );
        }

        $this->debugMsg( $relationship );
        // if the products are in the correct $alphaProductId already return true
        if( $this->checkDuplicateRelationship( $alphaProductId, $betaProductId ) && $overwritePreviousRelationship === false )
        {
            $this->debugMsg( "\tRelationship already exists", 2 );

            return false;
        }
        if( $alphaProductId == $betaProductId )
        {
            YiiItcher::log( 'Unable to create relationship between' . $alphaProductId . ' and ' . $betaProductId . ' same id', 'error', 'system.console.productDuplicationDebug' );

            return false;
        }

        switch( $relationship )
        {
            case self::MASTER_MASTER_RELATIONSHIP:
                if( !isset( $alphaProductRelationships[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) && !isset( $betaProductRelationships[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
                {
                    $this->debugMsg( "\tProducts incorrect type", 2 );

                    // if either product isn't a master then bail
                    return false;
                }
                // if we're linking two masters as duplicates
                if( $this->checkDuplicateRelationship( $alphaProductId, $betaProductId, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP ) )
                {
                    // if they have been resolved as not duplicates
                    if( $overwritePreviousRelationship )
                    {
                        // and we've been told to overwrite that delete the previous relationship
                        $this->deleteDuplicateRelationship( $alphaProductId, $betaProductId, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP );

                    }
                    else
                    {
                        $this->debugMsg( "\tProducts have existing alternate relationship", 2 );

                        // if we haven;t been told to overwrite then bail
                        return false;
                    }
                }

                break;

            case self::SUBORDINATE_MASTER_RELATIONSHIP:
                // if alpha is a master and beta is something that can be set as a subordinate

                if( $this->checkDuplicateRelationship( $inputAlpha, $inputBeta, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP ) )
                {
                    // if they have been resolved as not duplicates
                    if( $overwritePreviousRelationship )
                    {
                        // and we've been told to overwrite that delete the previous relationship
                        $this->deleteDuplicateRelationship( $alphaProductId, $betaProductId, self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP );
                    }
                    else
                    {
                        $this->debugMsg( "\tProducts have existing alternate relationship", 2 );

                        // if we haven;t been told to overwrite then bail
                        return false;
                    }
                }

                // remove any relation with another master
                if( isset( $betaProductRelationships[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) )
                {
                    foreach( $betaProductRelationships[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] as $masterProductId )
                    {
                        $this->deleteDuplicateRelationship( $masterProductId, $betaProductId, self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID );

                    }
                }
                $this->_productAssemblerService->assembleProductByProductId( $betaProductRelationships[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY], true );

                // if beta was a subordinate make it conflicted between alpha and the other master
                if( isset( $betaProductRelationships[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) && count( $betaProductRelationships[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] ) > 1 )
                {
                    foreach( $betaProductRelationships[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY] as $masterProduct )
                    {
                        if( $inputAlpha != $masterProduct )
                        {
                            $this->createDuplicateRelationship( $inputAlpha, $masterProduct, self::CONFLICTED_MASTER_PRODUCT_IDS_KEY );
                        }
                    }

                }

                $relationshipType = self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID;
                break;
            case self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP:

                // if the products are currently in a relationship
                if( $overwritePreviousRelationship )
                {
                    // if told to overwrite the relationship
                    switch( $currentRelationship )
                    {
                        case self::MASTER_MASTER_RELATIONSHIP:
                            // delete the master-master relationship
                            $this->deleteDuplicateRelationship( $inputAlpha, $inputBeta, self::MASTER_MASTER_RELATIONSHIP );
                            break;
                        case self::SUBORDINATE_MASTER_RELATIONSHIP:
                            // delete the master-subordinate relationship
                            $this->deleteDuplicateRelationship( $inputAlpha, $inputBeta, self::SUBORDINATE_MASTER_RELATIONSHIP );
                            break;
                    }
                }
                else
                {
                    $this->debugMsg( "\tProducts have existing alternate relationship", 2 );

                    // if not told to overwrite then bail
                    return false;
                }


                $relationshipType = self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID;
                break;
            default:
                $this->debugMsg( "\tInvalid relationship", 2 );
                $this->debugMsg( $relationship );
                return false;
        }

        //create the relationship
        $this->createProductRelation( $alphaProductId, $relationshipType, $betaProductId );
        $this->debugMsg( "\tRelationship successfully created", 2 );

        return true;
    }

    /**
     * function deletes a specified relationship between two products, returns true on success or false on failure
     *
     * @param int    $inputAlpha
     * @param int    $inputBeta
     * @param string $relationship
     *
     * @return bool
     */
    public function deleteDuplicateRelationship( $inputAlpha, $inputBeta, $relationship )
    {
        //check if the relationship exists
        $this->debugMsg( "Deleting relationship: $relationship :between: $inputAlpha and $inputBeta" );
        // if the relationship doesn't exist
        if( !$this->checkDuplicateRelationship( $inputAlpha, $inputBeta, $relationship ) )
        {
            $this->debugMsg( 'returning false' );
            return false;
        }

        switch( $relationship )
        {

            case self::SUBORDINATE_MASTER_RELATIONSHIP:
            case self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID:

                // remove any ratings or reviews that can be identified as coming from beta from alpha then queue alpha to the assembler
                $this->removeSubordinateRatingsAndReviewsFromMaster( $inputAlpha, $inputBeta );
                $this->_productAssemblerService->assembleProductByProductId( $alphaProductId, true );

                // if beta was a subordinate make it unique
                //$betaProductRelations->setAttribute( 'product_name', self::UNIQUE_PRODUCT_NAME );
                $this->markProductNotArchivedByProductId( $inputBeta );

                break;

        }

        //delete the relationship,
        $relationship = ProductRelation::model()
                                       ->findByAttributes( array('product_id' => $inputAlpha, 'related_product_id' => $inputBeta) );


        if( $relationship != null && $relationship->delete() )
        {
            //delete relations from cache, by deleting alphas betas will be deleted as well
            $this->debugMsg( 'deleting relaitons for ' . $inputAlpha );
            $this->deleteCachedRelations( $inputAlpha, true );
            return true;

        }
        else
        {
            YiiItcher::log( 'Unable to delete relationship between' . $inputAlpha . ' and ' . $inputBeta, 'error', 'system.console.productDuplicationDebug' );

            return false;
        }

    }

    /**
     * Function allows repairDuplicateRelationships to be queued or used from a queue
     *
     * @param array      $params
     * @param bool|false $queueOnly
     */
    public function repairDuplicateRelationshipsQueued( array $params, $queueOnly = false )
    {
        // if queueing
        if( $queueOnly )
        {
            // queue it
            $this->queueDataUpdate( $this->dataProviderId, __FUNCTION__, $params, self::MEDIUM_PRIORITY_QUEUE_ID );
        }
        // else if given the correct format
        elseif( isset( $params['product_id'] ) )
        {
            // execute
            if( isset( $params['repair'] ) )
            {
                $this->repairDuplicateRelationships( $params['product_id'], $params['repair'] );
            }
            else
            {
                $this->repairDuplicateRelationships( $params['product_id'] );
            }
        }
    }

    /**
     *
     * @todo ask Brandon
     * @todo TEst
     * function attempts to repair a broken product
     *
     * @param DataProviderProduct|int $productId
     * @param bool                    $attemptRepair
     *
     * @return null
     */
    public function repairDuplicateRelationships( $productId, $attemptRepair = true )
    {
        // check the input is valid (and get the PDDPP if we've been given an id)
        $productRelations = $this->getProductRelations( $productId );
        if( empty( $productRelations ) )
        {
            return null;
        }
        $this->debugMsg( "Repairing relationships for: " . $productId );

        $attemptReplacementProductsArray = array();

        if( !empty( $productRelations ) )
        {
            // if there are any relationships
            foreach( $productRelations as $relationshipIdsKey => $relationshipProductIds )
            {
                // for each relationship type
                switch( $relationshipIdsKey )
                {
                    // try to get the name of the relationship
                    case self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY:
                        $relationship = self::MASTER_MASTER_RELATIONSHIP;
                        break;
                    case self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY:
                    case self::SUBORDINATE_MASTER_PRODUCT_ID_KEY:
                    case self::CONFLICTED_MASTER_PRODUCT_IDS_KEY:
                        $relationship = self::SUBORDINATE_MASTER_RELATIONSHIP;
                        break;
                    case self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY;
                        $relationship = self::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP;
                        break;
                }
                if( isset( $relationship ) )
                {
                    if( is_array( $relationshipProductIds ) )
                    {
                        foreach( $relationshipProductIds as $relationshipProductId )
                        {
                            // for each relationship of that type try to delete the relationship
                            $deletionStatus = $this->deleteDuplicateRelationship( $productId, $relationshipProductId, $relationship );
                            if( $deletionStatus or ( $deletionStatus === false && $attemptRepair ) )
                            {
                                // if the relationship was deleted or at least the other product exists and we want to attempt repair save the relationship to the list to try to replace
                                $attemptReplacementProductsArray[] = array(
                                    'product_id'   => $relationshipProductId,
                                    'relationship' => $relationship
                                );
                            }
                        }
                    }
                    else
                    {
                        // for each relationship of that type try to delete the relationship
                        $deletionStatus = $this->deleteDuplicateRelationship( $productId, $relationshipProductIds, $relationship );
                        if( $deletionStatus or ( $deletionStatus === false && $attemptRepair ) )
                        {
                            // if the relationship was deleted or at least the other product exists and we want to attempt repair save the relationship to the list to try to replace
                            $attemptReplacementProductsArray[] = array(
                                'product_id'   => $relationshipProductIds,
                                'relationship' => $relationship
                            );
                        }
                    }
                }
                // tidy up the name for the next relationship type
                unset( $relationship );
            }
        }
        unset( $inputProductRelationships );

        // reset the product to a new blank state
        $this->debugMsg( "\tResetting relationships", 2 );
        /**
         * unique products are no longer in sue
         *
         * if( $inputProductDuplicationDataProviderProduct->product_name != self::MASTER_PRODUCT_NAME )
         * {
         * $inputProductDuplicationDataProviderProduct->setAttribute( 'product_name', self::UNIQUE_PRODUCT_NAME );
         * }
         * $inputProductDuplicationDataProviderProduct->setAttribute( 'product_attributes', null );
         * $inputProductDuplicationDataProviderProduct->save();**/

        $success = true;

        // attempt to recreate all the relationships that were deleted
        foreach( $attemptReplacementProductsArray as $attemptReplacementProduct )
        {
            if( !$this->createDuplicateRelationship( $productId, $attemptReplacementProduct['product_id'], $attemptReplacementProduct['relationship'] ) )
            {
                $success = false;
            }
        }

        // if the product is a master queue it to the assembler with a force update flag
        if( $this->checkIsMaster( $productId ) )
        {
            $this->_productAssemblerService->assembleProductByProductId( $productId, true );
        }

        return $success;
    }

    /**
     * function attempts to remove ratings and reviews from a master product that come from a subordinate product.
     *
     * @param DataProviderProduct|int $subordinateInput
     * @param DataProviderProduct|int $masterInput
     *
     * @throws InvalidArgumentException
     *
     * @throws CDbException
     */
    public function removeSubordinateRatingsAndReviewsFromMaster( $masterProductId, $subordinateProductId )
    {

        // get the subordinate's PDDPP in order to check that the relationship is correct
        if( !$this->checkDuplicateRelationship( $masterProductId, $subordinateProductId, self::SUBORDINATE_MASTER_RELATIONSHIP ) )
        {
            // if the subordinate isn't a subordinate or the two products aren't in a subordinate master relationship then exit
            return false;
        }

        $this->debugMsg( "Removing reviews for: $subordinateProductId from: $masterProductId" );

        // SQL to get the ratings of the subordinate product
        $subordinateRatingsSQL = "SELECT user_id, rating, last_updated FROM user_rating WHERE product_id = $subordinateProductId;";

        // if some ratings are returned
        if( $subordinateRatingResults = $this->db->createCommand( $subordinateRatingsSQL )
                                                 ->queryAll()
        )
        {
            // arrays to hold data
            $subordinateRatingUserIdsArray = array();
            $subordinateRatingsLastUpdatedArray = array();
            $subordinateRatingRatingsArray = array();

            foreach( $subordinateRatingResults as $SubordinateRating )
            {
                // extract data from each rating
                $subordinateRatingUserIdsArray[] = $SubordinateRating['user_id'];
                $subordinateRatingRatingsArray[$SubordinateRating['user_id']] = $SubordinateRating['rating'];
                $subordinateRatingsLastUpdatedArray[$SubordinateRating['user_id']] = $SubordinateRating['last_updated'];
            }

            // SQL to get the corresponding user ratings for the master product
            $masterRatingsSQL = "SELECT user_id, rating, last_updated FROM user_rating WHERE product_id = $masterProductId AND
user_id IN (" . implode( ',', $subordinateRatingUserIdsArray ) . ");";
            // if some ratings are returned
            if( $masterRatingResults = $this->db->createCommand( $masterRatingsSQL )
                                                ->queryAll()
            )
            {
                // array to hold ids
                $masterRatingFromSubordinateUserIdsArray = array();

                foreach( $masterRatingResults as $masterRating )
                {
                    // if the returned rating matches a rating from the subordinate add it to the array
                    if( $subordinateRatingsLastUpdatedArray[$masterRating['user_id']] == $masterRating['last_updated'] && $subordinateRatingRatingsArray[$masterRating['user_id']] == $masterRating['rating'] )
                    {
                        $masterRatingFromSubordinateUserIdsArray[] = $masterRating['user_id'];
                    }
                }
                // if there are any matching ratings
                if( !empty( $masterRatingFromSubordinateUserIdsArray ) )
                {
                    // SQL to get reviews from the subordinate
                    $subordinateReviewsSQL = "SELECT user_id, review_note, last_updated, review_date FROM user_review WHERE product_id = $subordinateProductId
                                              AND user_id IN (" . implode( ',', $masterRatingFromSubordinateUserIdsArray ) . ");";

                    // if we got any reviews from the subordinate
                    if( $subordinateReviewResults = $this->db->createCommand( $subordinateReviewsSQL )
                                                             ->queryAll()
                    )
                    {
                        // arrays to store information
                        $subordinateReviewsLastUpdatedArray = array();
                        $subordinateReviewReviewDatesArray = array();
                        $subordinateReviewReviewNotesArray = array();

                        foreach( $subordinateReviewResults as $SubordinateReview )
                        {
                            // extract the information from each found review
                            $subordinateReviewReviewNotesArray[$SubordinateReview['user_id']] = $SubordinateReview['review_note'];
                            $subordinateReviewsLastUpdatedArray[$SubordinateReview['user_id']] = $SubordinateReview['last_updated'];
                            $subordinateReviewReviewDatesArray[$SubordinateReview['user_id']] = $SubordinateReview['review_date'];
                        }
                    }

                    // SQL to get reviews from the master
                    $masterReviewsSQL = "SELECT user_id, review_note, last_updated, review_date FROM user_review WHERE product_id = $masterProductId
                                              AND user_id IN (" . implode( ',', $masterRatingFromSubordinateUserIdsArray ) . ");";

                    // if we found some master reviews
                    if( $masterReviewResults = $this->db->createCommand( $masterReviewsSQL )
                                                        ->queryAll()
                    )
                    {
                        // arrays to store information
                        $masterReviewFromSubordinateUserIdsArray = array();
                        $masterReviewUserIdsArray = array();

                        foreach( $masterReviewResults as $masterReview )
                        {
                            // store user id
                            $masterReviewUserIdsArray[] = $masterReview['user_id'];
                            if( isset( $subordinateReviewsLastUpdatedArray[$masterReview['user_id']] ) && $subordinateReviewsLastUpdatedArray[$masterReview['user_id']] == $masterReview['last_updated'] && isset( $subordinateReviewReviewDatesArray[$masterReview['user_id']] ) && $subordinateReviewReviewDatesArray[$masterReview['user_id']] == $masterReview['review_date'] && isset( $subordinateReviewReviewNotesArray[$masterReview['user_id']] ) && $subordinateReviewReviewNotesArray[$masterReview['user_id']] == $masterReview['review_note'] )
                            {
                                // if there is a matching review of the subordinate store the id
                                $masterReviewFromSubordinateUserIdsArray[] = $masterReview['user_id'];
                            }
                        }

                        // list of ratings from the subordinate without a review on the master
                        $otherRatingsToDelete = array_diff( $masterRatingFromSubordinateUserIdsArray, $masterReviewUserIdsArray );

                        if( !empty( $masterReviewFromSubordinateUserIdsArray ) )
                        {
                            // delete reviews and ratings from the master that match reviews and ratings on the subordinate
                            $deleteReviewsSQL = "DELETE FROM user_review WHERE product_id = $masterProductId AND user_id IN (" . implode( ',', $masterReviewFromSubordinateUserIdsArray ) . ");";
                            $deleteRatingsSQL = "DELETE FROM user_rating WHERE product_id = $masterProductId AND user_id IN (" . implode( ',', $masterReviewFromSubordinateUserIdsArray ) . ");";

                            $this->db->createCommand( $deleteReviewsSQL )
                                     ->execute();
                            $this->db->createCommand( $deleteRatingsSQL )
                                     ->execute();
                        }
                        if( !empty( $otherRatingsToDelete ) )
                        {
                            // delete ratings from the master that match ratings from the subordinate but do not have a review from their user
                            $deleteOtherRatingsSQL = "DELETE FROM user_rating WHERE product_id = $masterProductId AND user_id IN (" . implode( ',', $otherRatingsToDelete ) . ");";
                            $this->debugMsg( $deleteOtherRatingsSQL );
                            $this->db->createCommand( $deleteOtherRatingsSQL )
                                     ->execute();
                        }
                    }
                    else
                    {
                        // if there are no reviews found then just delete all ratings from the subordinate
                        $deleteRatingsSQL = "DELETE FROM user_rating WHERE product_id = $masterProductId AND user_id IN (" . implode( ',', $masterRatingFromSubordinateUserIdsArray ) . ");";
                        $this->debugMsg( $deleteRatingsSQL );
                        $this->db->createCommand( $deleteRatingsSQL )
                                 ->execute();
                    }
                }
            }
        }
    }

    /**
     * marks a product as archived
     *
     * @param $productId
     */
    protected function markProductArchivedByProductId( $productId )
    {
        // get the product
        $product = Product::model()
                          ->findByPk( $productId );

        if( $product )
        {
            // mark it archived and not to display
            $product->setAttribute( 'archived', 1 );
            $product->setAttribute( 'display', 0 );
            $product->save();

            // queue to clear the cache for the product and update the search index
            $this->messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $productId, true );
            $this->debugMsg( "Marked product archived: $productId" );
        }
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
        $this->debugMsg( $productId );
        $product = Product::model()
                          ->findByPk( $productId );


        if( $product )
        {
            //mark it not archived
            $product->setAttribute( 'archived', 0 );
            if( strlen( $product->product_description ) && $product->product_description != 'No description yet' )
            {
                // if it has a description set it to display
                $product->setAttribute( 'display', 1 );
            }
            $product->save();

            // queue to clear the cache for the product and update the search index
            $this->messageQueue->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $productId, true );
            $this->debugMsg( "Marked product not archived: $productId" );
        }
        else
        $this->debugMsg( 'product not found' );
    }

    protected function getSubCategories( $categoryId, $queueOnly )
    {

    }

    protected function getProducts( $categoryId, $queueOnly )
    {

    }

    protected function getRelatedProducts( $productData, $queueOnly )
    {

    }

    protected function searchProducts( $categoryId, $productName, $queueOnly )
    {

    }

    public function getExtendedInformation( &$product )
    {

    }

    /**
     * Function created to process current product relations stored as a ProductAttribute into ProductRelation
     *
     * @param int  $start     Controls the query start
     * @param int  $limit     Controls the query limit
     * @param bool $queueOnly Whether the function should be queued or not, defaults to true
     *
     * @return bool|void
     * @throws Exception
     */
    public function processExistingRelations( $start = 0, $limit = 50, $queueOnly = true )
    {

        //if queue only is true queue this calll
        if( $queueOnly === true )
        {
            $queue = new MessageQueue();
            $params = json_encode( array('start' => $start, 'limit' => $limit, 'queue_only' => false) );
            $queue->queueMessage( self::QUERY_QUEUE_NAME, $params );

            return true;
        }

        //select all products under DP 35 with product attributes set within out start limit
        $sql = "select product_id, product_name, product_attributes from data_provider_product where data_provider_id = 35  and product_attributes is not null  limit   " . $start . ", " . $limit;

        $duplicatedProducts = $this->db->createCommand( $sql )
                                       ->queryAll();
        //insert each record into the new product_relation table
        foreach( $duplicatedProducts as $duplicate )
        {
            //get the product id
            $productId = $duplicate['product_id'];

            //get the relationship type
            $relationshipType = $this->getRelationshipType( $duplicate['product_name'] );

            //if the relation is valid
            if( $relationshipType != false )
            {
                //get the related product ids
                $relatedProductIds = $this->getRelatedProductIds( $duplicate['product_attributes'], $relationshipType );

                //if there are related products
                if( is_array( $relatedProductIds ) )
                {
                    //use array keys as relationship type index as json array is bidimensional
                    foreach( $relatedProductIds as $relationshipType => $relatedProductList )
                    {

                        foreach( $relatedProductList as $relatedProductId )
                        {
                            //if data is valid
                            if( is_numeric( $relatedProductId ) && $relatedProductId != 0 )
                            {
                                //attempt to create the relation
                                $this->createProductRelation( $productId, $relationshipType, $relatedProductId );
                            }
                            else
                            {
                                //log failure and return false
                                YiiItcher::log( 'Unable to create relation between ' . $productId . ' and (CurrentData:' . json_encode( $duplicate ) . ')', 'warning', 'system.console.productDuplicationDebug' );

                            }

                        }
                    }
                }
            }
        }
        //queue next if we haven't reached the end of the table (selectedRows < limit)
        if( count( $duplicatedProducts ) === $limit )
        {
            $start += $limit;
            $this->processExistingRelations( $start, $limit );
        }

        return true;
    }

    /**
     * Create a product relation, if the relation already exists or is invalid false will be returned
     *
     * @param int  $productId        The main product id
     * @param int  $relationShipType The relationship type
     * @param int  $relatedProductId The product to be related
     * @param bool $queueOnly        Whether the function should run or queue (if true a queue item will be created for later processing)
     *
     * @return bool
     * @throws Exception
     */
    public function createProductRelation( $productId, $relationShipType, $relatedProductId, $queueOnly = false )
    {

        //queue the item if required
        if( $queueOnly == true )
        {
            $queue = new MessageQueue();
            $params = json_encode( array('product_id' => $productId, 'relationship_type' => $relationShipType, 'related_product_id' => $relatedProductId) );
            $queue->queueMessage( self::INSERT_QUEUE_NAME, $params );

            return true;
        }

        $this->debugMsg( 'test' );
        //query for relations between these two products
        $criteria = new CDbCriteria();
        $criteria->condition = ' (product_id=:productId and related_product_id=:relatedProductId) or (product_id=:relatedProductId and related_product_id=:productId) ';
        $criteria->params = array(':productId' => $productId, ':relatedProductId' => $relatedProductId);

        //check for existing relation
        $productRelation = ProductRelation::model()
                                          ->find( $criteria );

        //if there is no relation attempt to create one
        if( $productRelation == null )
        {
            try
            {
                //create the relation
                $productRelation = new ProductRelation();
                $productRelation->setAttributes( array(
                    'product_id'         => $productId,
                    'related_product_id' => $relatedProductId,
                    'relationship_type'  => $relationShipType
                ) );

                //if successful return true
                if( $productRelation->save() )
                {
                    $this->debugMsg( 'Created relation between ' . $productId . ' and ' . $relatedProductId . '(relation_id: ' . $productRelation->getPrimaryKey() );

                    return true;
                }
                //log failure
                else
                {
                    $this->debugMsg( 'Error occurred when processing ' . $productId . ' and ' . $relatedProductId );
                    YiiItcher::log( 'Unable to create relation between ' . $productId . ' and ' . $relatedProductId, 'warning', 'system.console.productDuplicationDebug' );

                    return false;
                }
            }
                //catch exception and log it on failure
            catch( CDbException $exception )
            {
                $this->debugMsg( 'Exception occurred when processing ' . $productId . ' and ' . $relatedProductId . '(' . $exception->getCode() . ' - ' . $exception->getMessage() . ')' );
                YiiItcher::log( 'Exception occurred when processing ' . $productId . ' and ' . $relatedProductId . '(' . $exception->getCode() . ' - ' . $exception->getMessage() . ')', 'error', 'system.console.productDuplicationDebug' );

                return false;
            }
        }
        //if a relation existed log this case for review
        else
        {
            $this->debugMsg( 'existed' );
            //if the relation is a different type from what we attempted to create log the diffence
            if( $relationShipType != $productRelation->getAttribute( 'relationship_type' ) )
            {
                $this->debugMsg( 'diff' );
                $this->debugMsg( $productRelation->getAttributes() );
                YiiItcher::log( 'Relation between ' . $productId . ' and ' . $relatedProductId . ' already existed (intended relation type:' . $relationShipType . ', current relation type ;' . $productRelation->getAttribute( 'relationship_type' ), 'warning', 'system.console.productDuplicationDebug' );
            }
            //log that the relation would be duplicate if not
            else
            {

                $this->debugMsg( 'same' );
                YiiItcher::log( 'Relationship between ' . $productId . ' and ' . $relatedProductId . ' already existed', 'info', 'system.console.productDuplicationDebug' );
            }

            return false;
        }
    }

    private function getRelationshipType( $currentValue )
    {

        switch( $currentValue )
        {
            case self::MASTER_PRODUCT_NAME:
                return self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID;
                break;
            case self::SUBORDINATE_PRODUCT_NAME:
                return self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID;
                break;
            case self::CONFLICTED_PRODUCT_NAME:
                return self::CONFLICTED_RELATION_ID;
                break;
            case self::UNIQUE_PRODUCT_NAME:
                return self::MARKED_AS_UNIQUE_RELATION_ID;
                break;
            default:
                return false;
        }
    }

    private function getRelatedProductIds( $json, $relationshipType )
    {
        //decode the json var
        $decodedValues = json_decode( $json, true );
        $returnArray = array();

        switch( $relationshipType )
        {
            case self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID:
                //if the product had subordinates compile them in an array, a master product
                if( isset( $decodedValues[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
                {
                    foreach( $decodedValues[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] as $value )
                    {
                        //mark the relation as master_subordinate
                        $returnArray[self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID][] = $value;
                    }
                }
                //a master product may have duplicates
                //if the product had subordinates compile them in an array, a master product
                if( isset( $decodedValues[self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY] ) )
                {
                    foreach( $decodedValues[self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY] as $value )
                    {
                        $returnArray[self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID][] = $value;
                    }
                }
                break;
            case self::CONFLICTED_RELATION_ID:

                //if the product had subordinates compile them in an array
                if( isset( $decodedValues[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] ) )
                {
                    foreach( $decodedValues[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY] as $value )
                    {
                        $returnArray[self::CONFLICTED_MASTER_RELATION_ID][] = $value;
                    }
                }
                break;

            case self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID:
                //if the product had subordinates compile them in an array
                if( isset( $decodedValues[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] ) )
                {
                    foreach( $decodedValues[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY] as $value )
                    {
                        $returnArray[self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID][] = $value;
                    }
                }
                break;

            case self::MARKED_AS_UNIQUE_RELATION_ID:
                if( !empty( $decodedValues ) && !is_null( $decodedValues ) )
                {
                    YiiItcher::log( $json . ' for unique product is not null' );
                }
                $returnArray = false;
                break;
            default:
                YiiItcher::log( 'Unable to find action for ' . $relationshipType . ' with json ' . $json, 'info', 'system.console.productDuplicationDebug' );
                $returnArray = false;
                break;

        }

        return $returnArray;
    }

    /**
     * Retrieves a sorted array of a products relations
     *
     * @param bool|int $productId   The product id in question, if unspecified $this->productId will be used if set
     * @param bool     $deleteCache Whether cache data for this products relations should be deleted and regenerated
     *
     * @return array With the products relations, or empty if there are none
     */
    public function getProductRelations( $productId = false, $deleteCache = false )
    {
        //delete cache if required
        if( $deleteCache === true )
        {
            $this->debugMsg( 'deleting cache' );
            $this->deleteCachedRelations( $productId );
        }

        //if $productId is not set default to $this->productId
        if( $productId === false )
        {
            $productId = $this->productId;
        }

        $this->debugMsg( $productId );
        //set the cache name
        $cacheKey = self::PRODUCT_RELATIONS_CACHE_NAME . "::$productId";

        //attempt to load cached data, if found return
        $cachedData = $this->cache->get( $cacheKey );
        if( $cachedData != false )
        {
            $this->debugMsg( "Retrieved relations from cache", 2 );

            return $cachedData;
        }

        //query the db for the products relations
        $criteria = new CDbCriteria();
        $criteria->condition = ' (product_id=:productId or related_product_id=:productId) ';
        $criteria->params = array(':productId' => $productId);
        $criteria->order = ' last_updated desc';

        $relations = array();
        $productRelations = ProductRelation::model()
                                           ->findAll( $criteria );

        //sort the results
        $relations = $this->sortProductRelations( $productRelations, $productId );

        $this->debugMsg( $relations );
        //save in cache
        $this->cache->set( $cacheKey, $relations, self::CACHE_TIME_ONE_WEEK );

        //return
        return $relations;
    }

    /**
     * @param array $productRelations
     * @param int   $productId
     *
     * @returns array $relations
     */
    private function sortProductRelations( $productRelations, $productId )
    {
        $relations = array();
        foreach( $productRelations as $productRelation )
        {
            if( $this->lastUpdated === null && $productId === $this->productId )
            {
                $this->lastUpdated = $productRelation->getAttribute( 'last_updated' );
            }

            switch( $productRelation->getAttribute( 'relationship_type' ) )
            {
                case self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID:
                    if( $productRelation->getAttribute( 'product_id' ) != $productId )
                    {
                        $relations[self::SUBORDINATE_MASTER_PRODUCT_ID_KEY][] = $productRelation->getAttribute( 'product_id' );
                    }
                    else
                    {
                        $relations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'related_product_id' );
                    }
                    break;

                case self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID:
                    if( $productRelation->getAttribute( 'product_id' ) === $productId )
                    {
                        $relations[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'related_product_id' );
                    }
                    else
                    {
                        $relations[self::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'product_id' );
                    }
                    break;

                case self::CONFLICTED_MASTER_RELATION_ID:
                    if( $productRelation->getAttribute( 'product_id' ) === $productId )
                    {
                        $relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'related_product_id' );
                    }
                    else
                    {
                        $relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'product_id' );
                    }
                    break;

                case self::MASTER_MASTER_RELATION_ID:
                    if( $productRelation->getAttribute( 'product_id' ) === $productId )
                    {
                        $relations[self::MASTER_MASTER_RELATIONSHIP][] = $productRelation->getAttribute( 'related_product_id' );
                    }
                    else
                    {
                        $relations[self::MASTER_MASTER_RELATIONSHIP][] = $productRelation->getAttribute( 'product_id' );
                    }
                    break;
                    break;
                case self::CONFLICTED_RELATION_ID:
                    if( $productRelation->getAttribute( 'product_id' ) === $productId )
                    {
                        $relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'related_product_id' );
                    }
                    else
                    {
                        $relations[self::CONFLICTED_MASTER_PRODUCT_IDS_KEY][] = $productRelation->getAttribute( 'product_id' );
                    }
                    break;
                default:
                    YiiItcher::log( 'Undefined case for  ' . $productId . ' and (CurrentData:' . json_encode( $productRelation ) . ')', 'warning', 'system.console.productDuplicationDebug' );
                    break;
            }
            //$relations[$productRelation->getAttribute( 'relationship_type' )][] = $productRelation->getAttributes();
        }
        foreach( $relations as $relationKey => $relationValues )
        {
            if( empty( $relations[$relationKey] ) )
            {
                unset( $relations[$relationKey] );
            }
        }

        return $relations;
    }

    /**
     * Checks product relations to see if a product is a master product
     *
     * @param int $productId The product id in question
     *
     * @return bool false unless the product is indeed a master
     */
    public function checkIsMaster( $productId )
    {
        $isMaster = false;

        //get a product Id
        if( !is_numeric( $productId ) )
        {
            return false;
        }
        $productRelations = $this->getProductRelations( $productId );
        if( $productRelations != false && !empty( $productRelations ) )
        {
            if( isset( $productRelations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) && !empty( $productRelations[self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Deletes cached relation for a product and it's related products
     *
     * @param int $productId
     */
    public function deleteCachedRelations( $productId, $recurse = false )
    {

        //get the products current relations
        $relations = $this->getProductRelations( $productId );

        //delete the relations
        $cacheKey = self::PRODUCT_RELATIONS_CACHE_NAME . "::$productId";
        $this->debugMsg( 'deleting ' . $cacheKey );
        $this->cache->set( $cacheKey, false );
        $this->debugMsg( $this->cache->get( $cacheKey ) );

        if( $recurse )
        {
            //delete cache for related products as well so as to avoid getting old cache which may be invalid
            foreach( $relations as $relationShipType => $relationships )
            {
                if( is_array( $relationships ) && !empty( $relationships ) )
                {
                    foreach( $relationships as $relationship )
                    {
                        $this->deleteCachedRelations( $relationship );
                    }
                }
            }
        }

    }

    /**
     *
     * Get the array keys in which related products would be stored
     *
     * @param int $id
     *
     * @return bool|string The key if found, false if not
     */
    private function getRelationshipTypeFromId( $id )
    {
        switch( $id )
        {
            case self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID:
                return self::SUBORDINATE_MASTER_RELATIONSHIP;
                break;
            case self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID:
                return SELF::RESOLVED_AS_NOT_DUPLICATE_RELATIONSHIP;
                break;
            case self::CONFLICTED_MASTER_RELATION_ID:
                return SELF::CONFLICTED_MASTER_PRODUCT_IDS_KEY;
                break;
            case self::CONFLICTED_RELATION_ID:
                return SELF::CONFLICTED_MASTER_PRODUCT_IDS_KEY;
                break;
            case self::MASTER_MASTER_RELATION_ID:
                return SELF::MASTER_MASTER_RELATIONSHIP;
                break;
            default:
                return false;
        }
    }
    /**
     *
     * Get the array keys in which related products would be stored
     *
     * @param int $id
     *
     * @return bool|string The key if found, false if not
     */
    private function getRelationshipKeyTypeFromId( $id )
    {
        switch( $id )
        {
            case self::MASTER_SUBORDINATE_DUPLICATE_RELATION_ID:
                return SELF::MASTER_SUBORDINATE_PRODUCT_IDS_KEY;
                break;
            case self::MASTER_SUBORDINATE_NOT_DUPLICATE_RELATION_ID:
                return SELF::RESOLVED_AS_NOT_DUPLICATE_PRODUCT_IDS_KEY;
                break;
            case self::CONFLICTED_MASTER_RELATION_ID:
                return SELF::CONFLICTED_MASTER_PRODUCT_IDS_KEY;
                break;
            case self::CONFLICTED_RELATION_ID:
                return SELF::CONFLICTED_MASTER_PRODUCT_IDS_KEY;
                break;
            default:
                return false;
        }
    }

    private function getRelationshipKeyTypeFromString( $string )
    {
        switch( $string )
        {
            case self::SUBORDINATE_MASTER_RELATIONSHIP:
                return self::MASTER_SUBORDINATE_PRODUCT_IDS_KEY;
                break;
            case self::MASTER_MASTER_RELATIONSHIP:
                return self::MASTER_DUPLICATE_MASTER_PRODUCT_IDS_KEY;
                break;
            default:
                return $string;
                break;
        }
    }

    /**@todo this function should be removed once the product assembler is refactored* */

    public function isMasterProduct( $productId )
    {
        return $this->checkIsMaster( $productId );
    }
}
