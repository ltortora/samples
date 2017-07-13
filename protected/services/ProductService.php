<?php
/**
 * ProductService class file
 */

/**
 * Product Service application component.
 */
class ProductService extends CApplicationComponent
{
    use ServiceTrait;

    const RELATED_CACHE_TIME = 604800; // only check for related items once per week
    const DEFAULT_RECOMMENDATIONS_CACHE_TIME = 86400;
    const PRODUCT_CACHE_QUEUE_NAME = 'y_msg_queue_product_cache';
    const SEARCH_ADD_QUEUE_NAME = 'y_msg_queue_search_add';
    const CACHE_TIME_ONE_WEEK = 604800;

    const USER_PRODUCT_LIST_TYPE_ITCHLIST = 1;
    const USER_PRODUCT_LIST_TYPE_SCRAPLIST = 2;

    const ECOMMERCE_PROVIDER_TYPE_INDIVIDUAL_ITEM = 1;
    const ECOMMERCE_PROVIDER_TYPE_GENERAL_LINK = 2;
    const ECOMMERCE_PROVIDER_TYPE_IMMEDIATE_CONSUMPTION = 3;

    const IMAGE_SUMMARY_OPTION_ALL = 0;

    const IMAGE_SUMMARY_OPTION_UNIQUE = 1;

    const IMAGE_SUMMARY_OPTION_SINGLE = 2;

    const IMAGE_SUMMARY_OPTION_ASPECT_RATIO = 3;

    const ASPECT_RATIO_COMPARISON_LIMIT = 2.25;

    private $_productRecommendedUsers = null;

    public function __construct()
    {
        // get the default YII cache object and db connection
        $this->_db = YiiItcher::app()->db;
        $this->_cache = YiiItcher::app()->cache;
        $this->_readCache = YiiItcher::app()->readCache;
        $this->_dynamoDb = new A2DynamoDb();

        // get the root category for music
        $this->_musicRootCategoryId = $this->musicRootId();

        // get the root category for books
        $this->_booksRootCategoryId = $this->booksRootId();

        // get the root category for tv shows
        $this->_tvShowsRootCategoryId = $this->tvShowsRootId();

        // get the root category for movies
        $this->_moviesRootCategoryId = $this->moviesRootId();

        // get the root category for games
        $this->_gamesRootCategoryId = $this->gamesRootId();

        // set utf-8 string encoding
        mb_internal_encoding( "UTF-8" );
    }

    /**
     * Singleton create method
     *
     * @return ProductService instance
     */
    public static function create()
    {
        if( self::$_instance === null )
        {
            $reflection = new \ReflectionClass( __CLASS__ );
            self::$_instance = $reflection->newInstanceArgs( func_get_args() );
        }

        return self::$_instance;
    }

    /**
     * Function deletes additional information for images for a product to allow the images (and then the product) to be deleted.
     *
     * @param $productId
     *
     * @throws CDbException
     */
    public static function deleteProductImageHistogramsAndRestrictedSamples( $productId )
    {
        //command to delete image histograms
        $deleteHistogramsCommand = YiiItcher::app()->db->createCommand( "DELETE FROM product_image_histogram WHERE image_url_md5 = :urlMd5;" );

        //command to delete restricted image samples
        $deleteRestrictedSamplesCommand = YiiItcher::app()->db->createCommand( "DELETE FROM restricted_image_sample WHERE image_url_md5 = :urlMd5;" );

        // get the image Url Md5s, then for each image
        $imageUrlMd5sSql = "SELECT image_url_md5 FROM product_image WHERE product_id = ?;";
        foreach( YiiItcher::app()->db->createCommand( $imageUrlMd5sSql )
                                     ->queryColumn( [$productId] ) as $urlMd5 )
        {
            // delete its histogram
            $deleteHistogramsCommand->execute( array('urlMd5' => $urlMd5) );
            // delete and restricted image samples
            $deleteRestrictedSamplesCommand->execute( array('urlMd5' => $urlMd5) );
        }
    }

    /**
     * Function deletes A product's product_relation entries
     *
     * @param $productId
     *
     * @throws CDbException
     */
    public static function deleteProductRelations( $productId )
    {
        YiiItcher::app()->db->createCommand( "DELETE FROM product_relation WHERE product_id = :prdId OR related_product_id = :prdId" )
                            ->bindValue( ':prdId', $productId )
                            ->execute();
    }

    /**
     * Get the recommended products for a user
     *
     * @param        $args
     * @param string $limit
     * @param        $resetCache
     *
     * @return array
     */
    public function getRecommendedProductsForUser( $args, $limit, $resetCache )
    {
        $this->deltaTimer( 'START getRecommendedProductsForUser' );
        $subCategoryId = null;
        $ecommerceProviderFilters = null;

        // break the items on the slash
        $argBits = explode( ',', $args );

        // get the userId
        $userId = $argBits[0];

        // get the categoryId if it's there
        if( !empty( $argBits[1] ) )
        {
            $subCategoryId = $argBits[1];
        }

        // get the ecommerceProviderFilters if it's there
        if( !empty( $argBits[2] ) )
        {
            // ecommerceProviders will be like Netflix&iTunes
            $ecommerceProviders = explode( '&', $argBits[2] );

            // run through the list passed
            foreach( $ecommerceProviders as $ecommerceProvider )
            {
                // if we have an ID for the name
                if( $ecommerceProviderId = EcommerceProvider::model()
                                                            ->getIdByName( $ecommerceProvider )
                )
                {
                    // add it to the list
                    if( !empty( $ecommerceProviderFilters ) )
                    {
                        $ecommerceProviderFilters .= ',' . $ecommerceProviderId;
                    }
                    else
                    {
                        $ecommerceProviderFilters = $ecommerceProviderId;
                    }
                }
            }
        }

        // get the recommendations
        $recommendationService = new RecommendationService();
        if( $this->_debug )
        {
            $recommendationService->initDebugBuffer( 'user_recommendations' );
        }
        $products = $recommendationService->getUserRecommendationsBySimilarUsers( $userId, $subCategoryId, $ecommerceProviderFilters, $limit, $resetCache );
        $this->deltaTimer( 'END getRecommendedProductsForUser' );

        return $products;
    }

    /**
     * Get the review for the specified review id
     *
     * @param integer $reviewId
     *
     * @return array review details
     */
    public function getProductReview( $reviewId )
    {
        /**
         * @var $userReview UserReview
         */
        $userReview = UserReview::model()
                                ->with( 'user', 'user_rating' )
                                ->together()
                                ->findByPk( $reviewId );

        $reviewDetails = array();
        if( $userReview )
        {
            // get the review details
            $reviewDetails = $userReview->getAttributes();

            // add the user details
            $reviewDetails['user_name'] = ( strlen( $userReview->user->getAttribute( 'name' ) ) ? $userReview->user->getAttribute( 'name' ) : $userReview->user->getAttribute( 'username' ) );
            $reviewDetails['user_profile_picture'] = $userReview->user->getAttribute( 'user_profile_picture' );

            // if we don't have a picture for this user, and they're a non-itcher user
            if( $reviewDetails['user_profile_picture'] == null && $userReview->user->getAttribute( 'source' ) > 0 && strlen( trim( $reviewDetails['user_name'] ) ) )
            {
                // set the picture to be loaded asynchronously
                $reviewDetails['user_profile_picture'] = YiiItcher::app()->params['baseURI'] . '/image/user/' . $userReview->user->getAttribute( 'user_id' );
            }

            $reviewDetails['user_rating'] = null;
            if( $userReview->user_rating )
            {
                $reviewDetails['user_rating'] = $userReview->user_rating->getAttribute( 'rating' );
            }
        }

        return $reviewDetails;
    }

    /**
     * Get the list of reviews for a productId
     *
     * @param integer $productId
     * @param string  $limit
     *
     * @return array reviews
     */
    public function getProductReviews( $productId, $limit = '0,10' )
    {
        $offsetLimit = explode( ',', $limit );

        return $this->getUserReviews( $productId, $offsetLimit[0], $offsetLimit[1] );
    }

    /**
     * Get user reviews for a specific product
     *
     * @param integer $productId
     * @param integer $offset
     * @param integer $limit
     *
     * @return array $reviews
     */
    public function getUserReviews( $productId, $offset = 0, $limit = 10 )
    {
        $this->deltaTimer( 'start getUserReviews' );

        $criteria = new CDbCriteria( array(
            'order'  => 'review_date DESC',
            'offset' => $offset,
            'group'  => 'CONCAT(`name`, review_date, review_note)',
            'limit'  => $limit
        ) );
        /**
         * @var $userReviews UserReview[]
         */
        $userReviews = UserReview::model()
                                 ->with( 'user', 'user_rating' )
                                 ->together()
                                 ->findAllByAttributes( array(
                                     'product_id' => $productId
                                 ), $criteria );

        $this->deltaTimer( 'got reviews' );

        $reviews = array();
        if( $userReviews )
        {
            // holder for if we need to match blank titles
            $productNameLookups = array();

            // run through the userReviews and get the linked ratings
            foreach( $userReviews as $userReview )
            {
                // get the review details
                $reviewDetails = $userReview->getAttributes();

                // check if the review details contains "Five Stars" and remove it as necessary
                if( ( $fiveStarsPos = stripos( $reviewDetails['review_note'], 'five stars. ' ) ) !== false )
                {
                    // update the user_review record
                    $reviewDetails['review_note'] = str_ireplace( 'five stars. ', '', $reviewDetails['review_note'] );
                    $userReview->review_note = $reviewDetails['review_note'];
                    $userReview->save();
                }

                // check if the review details has an iLink with a blank product title - the front end won't format it
                if( strpos( $reviewDetails['review_note'], '[iLink name=' ) !== false )
                {
                    // get the contents
                    preg_match( "/\[iLink name='(.*)' id='(\d+)'\](.+)/", $reviewDetails['review_note'], $matches );

                    // if it's an iLink review
                    if( count( $matches ) > 1 )
                    {
                        // if the title is blank
                        if( empty( $matches[1] ) )
                        {
                            // if we don't already have thr product title in our lookup array
                            if( !isset( $productNameLookups[$matches[2]] ) )
                            {
                                /**
                                 * get the title from the product
                                 * @var $product Product
                                 */
                                $product = Product::model()
                                                  ->findByPk( $matches[2] );

                                // if we have the product
                                if( $product )
                                {
                                    // set the product name into our lookup array
                                    $productNameLookups[$matches[2]] = $product->product_name;
                                }

                            }

                            // if we have a valid product_id
                            if( isset( $productNameLookups[$matches[2]] ) )
                            {
                                // update the review with an iLink
                                $reviewDetails['review_note'] = "[iLink name='" . $productNameLookups[$matches[2]] . "' id='" . $matches[2] . "']" . $matches[3];
                            }
                            else
                            {
                                // update the review with an iHl
                                $reviewDetails['review_note'] = "[iHl]" . $productNameLookups[$matches[2]] . "[/iHl]" . $matches[3];
                            }

                            // update the user_review record
                            $userReview->review_note = $reviewDetails['review_note'];
                            $userReview->save();
                        }

                    }
                }

                // get the user source
                $source = 'itcher';
                $sourceBits = explode( '::', $userReview->user->username );
                if( count( $sourceBits ) > 1 )
                {
                    $source = $sourceBits[1];
                }

                // add the user details
                $userId = $userReview->user->getAttribute( 'user_id' );
                $reviewDetails['user_name'] = ( strlen( $userReview->user->getAttribute( 'name' ) ) ? $userReview->user->getAttribute( 'name' ) : $userReview->user->getAttribute( 'username' ) );
                $reviewDetails['source'] = $source;
                $reviewDetails['user_profile_picture'] = $userReview->user->getAttribute( 'user_profile_picture' );

                // set_error_handler(array('Utilities', 'functionErrorHandler'));

                // if we don't have a picture for this user, and they're a non-itcher user
                if( $reviewDetails['user_profile_picture'] == null && $userReview->user->getAttribute( 'source' ) > 0 && strlen( trim( $reviewDetails['user_name'] ) ) )
                {
                    // set the picture to be loaded asynchronously
                    $reviewDetails['user_profile_picture'] = YiiItcher::app()->params['imagesRootURI'] . 'image/user/id/' . $userId . '/width/40/title/' . $userId . '.jpg';
                }
                else
                {
                    if( $reviewDetails['user_profile_picture'] != null )
                    {
                        $reviewDetails['user_profile_picture'] = YiiItcher::app()->params['imagesRootURI'] . 'image/user/id/' . $userId . '/width/40/title/' . basename( $reviewDetails['user_profile_picture'] );
                    }
                }

                $reviewDetails['user_rating'] = null;
                if( $userReview->user_rating )
                {
                    $reviewDetails['user_rating'] = $userReview->user_rating->getAttribute( 'rating' );
                }

                // add to the array
                $reviews[] = $reviewDetails;
            }
        }
        $this->deltaTimer( 'done getUserReviews' );

        return $reviews;
    }

    /**
     * Get the list of likes for a reviewId
     *
     * @param integer $reviewId
     *
     * @return array $likes
     */
    public function getReviewLikes( $reviewId )
    {
        /**
         * @var $userReviewLikes UserReviewLike[]
         */
        $userReviewLikes = UserReviewLike::model()
                                         ->findAllByAttributes( array(
                                             'review_id' => $reviewId
                                         ) );
        $likes = array();
        if( $userReviewLikes )
        {
            // run through the models and get the linked ratings
            foreach( $userReviewLikes as $userReviewLike )
            {
                $like = $userReviewLike->getAttributes();
                $likes[] = $like;
            }
        }

        return $likes;
    }

    /**
     * Get the list of comments for a reviewId
     *
     * @param int    $reviewId
     * @param string $limit
     *
     * @return array
     */
    public function getReviewCommentLikes( $reviewId, $limit )
    {
        /**
         * @var $userCommentLikes UserCommentLike[]
         */
        $userCommentLikes = UserCommentLike::model()
                                           ->findAllByAttributes( array(
                                               'user_comment_id' => $reviewId
                                           ), array(
                                               'limit' => $limit
                                           ) );
        $likes = array();
        if( $userCommentLikes )
        {
            // run through the models and get the linked ratings
            foreach( $userCommentLikes as $userCommentLike )
            {
                $like = $userCommentLike->getAttributes();
                $likes[] = $like;
            }
        }

        return $likes;
    }

    /**
     * Get the list of comments for a reviewId
     *
     * @param int    $reviewId
     * @param string $limit
     *
     * @return array
     */
    public function getReviewComments( $reviewId, $limit = "0,10" )
    {
        $offsetLimit = explode( ',', $limit );
        $criteria = new CDbCriteria( array(
            'order'  => 'last_updated DESC',
            'offset' => $offsetLimit[0],
            'limit'  => $offsetLimit[1]
        ) );
        /**
         * @var $userComments UserComment[]
         */
        $userComments = UserComment::model()
                                   ->with( 'user' )
                                   ->together()
                                   ->findAllByAttributes( array(
                                       'review_id' => $reviewId
                                   ), $criteria );
        $comments = array();
        if( $userComments )
        {
            // run through the models and get the linked ratings
            foreach( $userComments as $userComment )
            {
                $comment = $userComment->getAttributes();
                $comment['user_name'] = ( strlen( $userComment->user->getAttribute( 'name' ) ) ? $userComment->user->getAttribute( 'name' ) : $userComment->user->getAttribute( 'username' ) );
                $comment['user_profile_picture'] = $userComment->user->getAttribute( 'user_profile_picture' );

                // if we don't have a picture for this user, and they're a non-itcher user
                if( $comment['user_profile_picture'] == null && $userComment->user->getAttribute( 'source' ) > 0 )
                {
                    // get the next free picture from the available facebook profiles, same gender if possible
                    $comment['user_profile_picture'] = YiiItcher::app()->params['baseURI'] . '/image/user/' . $userComment->user->getAttribute( 'user_id' );
                }

                $likes = UserCommentLike::model()
                                        ->countByAttributes( array(
                                            'user_comment_id' => $comment['user_comment_id'],
                                            'like_status'     => 1
                                        ) );
                $comment['num_likes'] = $likes;
                $dislikes = UserCommentLike::model()
                                           ->countByAttributes( array(
                                               'user_comment_id' => $comment['user_comment_id'],
                                               'like_status'     => 0
                                           ) );
                $comment['num_dislikes'] = $dislikes;

                $comments[] = $comment;
            }
        }

        return $comments;
    }

    /**
     * Get the data tabs containing the correct language variables
     *
     * @param null|int $productId
     * @param string   $languageCode
     * @param bool     $resetCache
     *
     * @return array data tabs array
     */
    public function getDataTabsForLanguage( $productId = null, $languageCode = 'en', $resetCache = false )
    {
        // define the cache key
        $cacheKey = __FUNCTION__ . ':20151216:' . ( is_null( $productId ) ? 0 : $productId ) . ':' . $languageCode;

        // get the data tabs from the LRU
        if( !( $tabs = $this->cacheGet( $cacheKey ) ) || $resetCache )
        {
            $this->debugMessage( ( "Miss LRU Cache $cacheKey" ) );
            // get the maximum set of data tabs for the category for the language
            $tabs = $this->getDataTabsForCategoryForLanguage( $productId, $languageCode, false, $resetCache );

            // if we were passed a product_id
            if( $productId )
            {
                // get the set of checked product attributes
                $this->getCheckedProductAttributes( $productId, $tabs, $resetCache );
            }

            // return the final values
            $tabs = array_values( $tabs );

            // set the item in the LRU
            $this->debugMessage( ( "Set LRU Cache $cacheKey" ) );
            $this->cacheSet( $cacheKey, $tabs );
        }
        else
        {
            $this->debugMessage( ( "Hit LRU Cache $cacheKey" ) );
        }

        return $tabs;
    }

    /**
     * Get the set of potentially available data tabs per country and per root_category_id
     *
     * @param int|null $productId
     * @param string   $languageCode
     * @param boolean  $showNewEcommerceLinks
     * @param boolean  $resetCache
     *
     * @return array
     */
    private function getDataTabsForCategoryForLanguage( $productId = null, $languageCode, $showNewEcommerceLinks, $resetCache = false )
    {
        // if we have been passed a product, get its root_category_id
        $rootCategoryId = 0;
        $productName = null;
        if( $productId )
        {
            /** @var Product $product */
            $product = Product::model()
                              ->findByPk( $productId );

            // if it doesn't exist, bail.
            if( !$product )
            {
                return array();
            }
            $rootCategoryId = $product->getRootCategoryId( $productId );
            $productName = $product->product_name;
        }

        // if the tab list is not in the cache
        $cacheKey = __CLASS__ . '::' . __FUNCTION__ . '::' . $languageCode . '::' . $rootCategoryId;
        if( !( $tabs = $this->cacheGet( $cacheKey ) ) || $resetCache )
        {
            // get the overall set of tabs from the db
            $tabSQL = "SELECT precedence, overview, product_attribute_type_name, language_string AS title
			FROM product_data_tab
			JOIN language_string ON product_data_tab.language_string_id=language_string.language_string_id
			JOIN product_attribute_type ON product_data_tab.product_attribute_type_id=product_attribute_type.product_attribute_type_id
			WHERE language_string.language_code = '$languageCode'
			AND display=1
			ORDER BY precedence DESC ";

            // execute the query
            $tabsData = $this->_db->createCommand( $tabSQL )
                                  ->queryAll();

            // put all the rows into an associative array
            $tabs = array();
            foreach( $tabsData as $tabsDataRow )
            {
                $tabs[$tabsDataRow['product_attribute_type_name']] = $tabsDataRow;
            }

            // set the result in the cache for an hour
            $this->cacheSet( $cacheKey, $tabs, self::$_CACHE_TIME_ONE_HOUR );
        }

        // get ecommerce links if we have them
        $ecommerceLinks = $this->getProductEcommerceLinks( $productId, $rootCategoryId );

        // if we have old style ecommerce shop links
        if( isset( $ecommerceLinks['ecommerce_links'] ) && !empty( $ecommerceLinks['ecommerce_links'] ) && !$showNewEcommerceLinks )
        {
            // add them to the data tabs
            $tabs['ecommerce_links'] = array(
                'precedence'                  => '99',
                'overview'                    => '0',
                'product_attribute_type_name' => 'ecommerce_links',
                'title'                       => 'Shop',
            );
        }
        // else if we have new style ecommerce shop links
        elseif( isset( $ecommerceLinks['ecommerce_product_links'] ) && !empty( $ecommerceLinks['ecommerce_product_links'] ) )
        {
            // add them to the data tabs
            $tabs['ecommerce_product_links'] = array(
                'precedence'                  => '99',
                'overview'                    => '0',
                'product_attribute_type_name' => 'ecommerce_product_links',
                'title'                       => 'Shop',
            );
        }

        // set up removal of attributes based on root category
        switch( $rootCategoryId )
        {
            // get the root category for music
            case $this->_musicRootCategoryId:

                // remove tabs which don't belong to the category
                $excludedProductAttributesArray = array('trailers', 'cast', 'crew', 'info', 'editorial_review', 'spotify_uri', 'rotten_tomatoes_uri');

                // update related items tab to be category and product specific if possible
                if( isset( $tabs['related_items'] ) )
                {
                    $tabs['related_items']['page_title'] = $tabs['related_items']['title'] = 'Similar Artists';
                    if( $productName )
                    {
                        $tabs['related_items']['page_title'] = 'Artists similar to ' . $productName;
                    }
                }
                break;

            // get the root category for books
            case $this->_booksRootCategoryId:

                // remove tabs which don't belong to the category
                $excludedProductAttributesArray = array(
                    'editorial_review',
                    'music_album',
                    'music_single',
                    'info',
                    'news',
                    'urls',
                    'tracks',
                    'trailers',
                    'spotify_uri',
                    'rotten_tomatoes_uri',
                    'cast',
                    'crew'
                );

                // update related items tab to be category and product specific if possible
                if( isset( $tabs['related_items'] ) )
                {
                    $tabs['related_items']['page_title'] = $tabs['related_items']['title'] = 'Similar Books';
                    if( $productName )
                    {
                        $tabs['related_items']['page_title'] = 'Books similar to ' . $productName;
                    }
                }
                break;

            // get the root category for movies
            case $this->_moviesRootCategoryId:

                // remove tabs which don't belong to the category
                $excludedProductAttributesArray = array('editorial_review', 'spotify_uri', 'music_album', 'music_single', 'news', 'urls', 'tracks');

                // update related items tab to be category and product specific if possible
                if( isset( $tabs['related_items'] ) )
                {
                    $tabs['related_items']['page_title'] = $tabs['related_items']['title'] = 'Similar Movies';
                    if( $productName )
                    {
                        $tabs['related_items']['page_title'] = 'Movies similar to ' . $productName;
                    }
                }
                break;

            // get the root category for tv shows
            case $this->_tvShowsRootCategoryId:

                // remove tabs which don't belong to the category
                $excludedProductAttributesArray = array('editorial_review', 'spotify_uri', 'music_album', 'music_single', 'news', 'urls', 'tracks');

                // update related items tab to be category and product specific if possible
                if( isset( $tabs['related_items'] ) )
                {
                    $tabs['related_items']['page_title'] = $tabs['related_items']['title'] = 'Similar TV Shows';
                    if( $productName )
                    {
                        $tabs['related_items']['page_title'] = 'TV Shows similar to ' . $productName;
                    }
                }
                break;

            // get the root category for games
            case $this->_gamesRootCategoryId:

                // remove tabs which don't belong to the category
                $excludedProductAttributesArray = array(
                    'cast',
                    'crew',
                    'editorial_review',
                    'spotify_uri',
                    'music_album',
                    'music_single',
                    'news',
                    'urls',
                    'tracks',
                    'rotten_tomatoes_uri'
                );

                // update related items tab to be category and product specific if possible
                if( isset( $tabs['related_items'] ) )
                {
                    $tabs['related_items']['page_title'] = $tabs['related_items']['title'] = 'Similar Games';
                    if( $productName )
                    {
                        $tabs['related_items']['page_title'] = 'Games similar to ' . $productName;
                    }
                }
                break;

            default:

                // remove tabs which don't belong to the category
                $excludedProductAttributesArray = array('editorial_review');
                break;
        }

        // add generic SEO text
        if( $productName )
        {
            if( isset( $tabs['product_description'] ) )
            {
                $tabs['product_description']['page_title'] = 'About ' . $productName;
            }
            if( isset( $tabs['all_images'] ) )
            {
                $tabs['all_images']['page_title'] = 'Images of ' . $productName;
            }
            if( isset( $tabs['music_album'] ) )
            {
                $tabs['music_album']['page_title'] = $productName . " Albums";
            }
            if( isset( $tabs['music_single'] ) )
            {
                $tabs['music_single']['page_title'] = $productName . " Singles";
            }
            if( isset( $tabs['news'] ) )
            {
                $tabs['news']['page_title'] = 'News Related to ' . $productName;
            }
            if( isset( $tabs['urls'] ) )
            {
                $tabs['urls']['page_title'] = $productName . ' Links';
            }
            if( isset( $tabs['cast'] ) )
            {
                $tabs['cast']['page_title'] = $productName . ' Cast';
            }
            if( isset( $tabs['info'] ) )
            {
                $tabs['info']['page_title'] = $productName . ' Facts';
            }
            if( isset( $tabs['crew'] ) )
            {
                $tabs['crew']['page_title'] = $productName . ' Crew';
            }
            if( isset( $tabs['trailers'] ) )
            {
                $tabs['trailers']['page_title'] = $productName . ' Trailers';
            }
            if( isset( $tabs['ecommerce_links'] ) )
            {
                $tabs['ecommerce_links']['page_title'] = 'Shop for ' . $productName;
            }
            if( isset( $tabs['video_reviews'] ) )
            {
                $tabs['video_reviews']['page_title'] = $productName . " Video Reviews";
            }
            if( isset( $tabs['tv_season'] ) )
            {
                $tabs['tv_season']['page_title'] = $productName . " Seasons";
            }
            if( isset( $tabs['tv_episode'] ) )
            {
                $tabs['tv_episode']['page_title'] = $productName . " Episodes";
            }
        }

        // discard attributes we don't want for this root_category_id
        $this->discardUnwantedAttributes( $tabs, $excludedProductAttributesArray );

        return $tabs;
    }

    /**
     * Get product e-commerce links
     *
     * @param int $productId
     * @param int $rootCategoryId
     *
     * @return array
     */
    public function getProductEcommerceLinks( $productId, $rootCategoryId )
    {
        $finalLinks = array();
        $ecommerceLinks = array();
        $ecommerceProductLinks = array();
        /** @var $importedEcommerceLinks [] $ecommerceProviderProducts */
        $importedEcommerceLinks = EcommerceProviderProduct::model()
                                                          ->with( 'ecommerceProvider' )
                                                          ->together()
                                                          ->findAllByAttributes( array('product_id' => $productId) );
        /** @var EcommerceProviderProductAdminOverride[] $adminEcommerceLinks */
        $adminEcommerceLinks = EcommerceProviderProductAdminOverride::model()
                                                                    ->with( 'ecommerceProvider' )
                                                                    ->together()
                                                                    ->findAllByAttributes( array('product_id' => $productId) );

        //order imported links by provider_id::country_code
        /** @var $ecommerceProviderProducts [] $ecommerceProviderProducts */
        $ecommerceProviderProducts = array();

        foreach( $importedEcommerceLinks as $importedEcommerceLink )
        {
            $ecommerceProviderProducts[$importedEcommerceLink->getAttribute( 'ecommerce_provider_id' ) . '::' . $importedEcommerceLink->getAttribute( 'country_code' )] = $importedEcommerceLink;
        }

        //order adminUser links by provider_id::country_code, if an imported link for the same country and provider exists, overwrite it
        foreach( $adminEcommerceLinks as $adminEcommerceLink )
        {
            $ecommerceProviderProducts[$adminEcommerceLink->getAttribute( 'ecommerce_provider_id' ) . '::' . $adminEcommerceLink->getAttribute( 'country_code' )] = $adminEcommerceLink;
            //unset excluded links
            if( $adminEcommerceLink->getAttribute( 'exclude' ) == true )
            {
                unset( $ecommerceProviderProducts[$adminEcommerceLink->getAttribute( 'ecommerce_provider_id' ) . '::' . $adminEcommerceLink->getAttribute( 'country_code' )] );
            }
        }

        if( $ecommerceProviderProducts )
        {
            $checkEcommerceProducts = array();
            foreach( $ecommerceProviderProducts as $ecommerceProviderProduct )
            {
                if( $ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_type_id == self::ECOMMERCE_PROVIDER_TYPE_INDIVIDUAL_ITEM )
                {
                    $ecommerceGroupLabel = "Buy it on";
                }
                else
                {
                    switch( $rootCategoryId )
                    {
                        case self::moviesRootId():
                            $ecommerceGroupLabel = "Watch it on";
                            break;
                        case self::tvShowsRootId():
                            $ecommerceGroupLabel = "Watch it on";
                            break;
                        case self::gamesRootId():
                            $ecommerceGroupLabel = "Play it on";
                            break;
                        case self::musicRootId():
                            $ecommerceGroupLabel = "Listen to it on";
                            break;
                        case self::booksRootId():
                            $ecommerceGroupLabel = "Read it on";
                            break;
                        default:
                            $ecommerceGroupLabel = "Read it on";
                            break;

                    }
                }

                // get the ecommerce link
                $ecommerceLink = $ecommerceProviderProduct->ecommerce_link;

                // if it's an old style Amazon link
                if( preg_match( '/(http:\/\/www\.amazon\.(?:com|co\.uk)\/)(.+)\/dp\/(.+)/', $ecommerceLink, $matches ) )
                {
                    /**
                     * We're looking for this output:
                     *
                     * array(
                     *   0 => http://www.amazon.co.uk/Invictus-Morgan-Freeman/dp/B00ET2BBOO%3FSubscriptionId%3DAKIAJAU2LKUSVO2JDH6Q%26tag%3Ditcher-21%26linkCode%3Dxm2%26camp%3D2025%26creative%3D165953%26creativeASIN%3DB00ET2BBOO
                     *   1 => http://www.amazon.co.uk/
                     *   2 => Invictus-Morgan-Freeman
                     *   3 => B00ET2BBOO%3FSubscriptionId%3DAKIAJAU2LKUSVO2JDH6Q%26tag%3Ditcher-21%26linkCode%3Dxm2%26camp%3D2025%26creative%3D165953%26creativeASIN%3DB00ET2BBOO
                     * )
                     *
                     */
                    if( count( $matches ) == 4 )
                    {
                        // reconstruct
                        $ecommerceLink = $matches[1] . 'gp/product/' . urldecode( $matches[3] );
                    }
                }

                // if we don't already have this ecommerce link for this country
                if( !isset( $checkEcommerceProducts[$ecommerceProviderProduct->country_code][$ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_name] ) )
                {
                    // put the link in
                    $ecommerceLinks[$ecommerceProviderProduct->country_code][] = array(
                        'ecommerce_provider_name'     => $ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_name,
                        'ecommerce_provider_link'     => $ecommerceLink,
                        'ecommerce_provider_logo_url' => $ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_logo_url,
                    );
                    $checkEcommerceProducts[$ecommerceProviderProduct->country_code][$ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_name] = 1;
                }

                // put the link in for the product
                $ecommerceProductLinks[$ecommerceProviderProduct->country_code][$ecommerceGroupLabel][$ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_name][] = array(
                    'ecommerce_product_provider_name'     => $ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_name,
                    'ecommerce_product_provider_logo_url' => $ecommerceProviderProduct->ecommerceProvider->ecommerce_provider_logo_url,
                    'ecommerce_product_name'              => $ecommerceProviderProduct->product_name,
                    'ecommerce_product_link'              => $ecommerceLink,
                    'ecommerce_product_image_url'         => $ecommerceProviderProduct->image_url,
                    'ecommerce_product_price'             => $ecommerceProviderProduct->price,
                    'ecommerce_product_currency_code'     => $ecommerceProviderProduct->currency_code,
                );
            }
        }

        // build final array
        if( !empty( $ecommerceLinks ) )
        {
            $finalLinks['ecommerce_links'] = $ecommerceLinks;
        }
        // build final array
        if( !empty( $ecommerceLinks ) )
        {
            $finalLinks['ecommerce_product_links'] = $ecommerceProductLinks;
        }

        // return final links
        return $finalLinks;

    }

    /**
     * Get the set of checked product attributes from the cache which match those in the data tabs
     *
     * @param int   $productId
     * @param array $attributeCheckArray
     * @param bool  $resetCache
     */
    private function getCheckedProductAttributes( $productId, &$attributeCheckArray, $resetCache = false )
    {
        // get the set of available attributes from redis, ignoring missing elements
        $checkedAttributes = $this->getProductAttributesByProductId( null, $productId, array_keys( $attributeCheckArray ), $resetCache );

        // parse the returned data
        foreach( $attributeCheckArray as $key => $value )
        {
            // if the requested key was not available
            if( !isset( $checkedAttributes[$key] ) )
            {
                // remove it from the array
                unset( $attributeCheckArray[$key] );
            }
        }
    }

    /**
     * Get product attributes by productId
     *
     * @param string|null $queryString - product_id=XX&attributes=attr1,attr2,attr3
     * @param int|null    $productId
     * @param array|null  $attributes
     * @param bool        $resetCache
     *
     * @return array
     * @throws CException
     */
    public function getProductAttributesByProductId( $queryString, $productId = null, $attributes = null, $resetCache = false )
    {
        $this->startTimer( 'getProductAttributesByProductId' );

        // if we have a querystring
        if( !empty( $queryString ) )
        {
            // parse the queryString
            parse_str( $queryString, $qsParams );

            // make sure we have a product_id and a list of elements
            if( !isset( $qsParams['product_id'] ) || !isset( $qsParams['attributes'] ) )
            {
                return array();
            }
            if( empty( $qsParams['product_id'] ) )
            {
                return array();
            }

            // set the product_id
            $productId = $qsParams['product_id'];

            // get the attributes
            $attributes = explode( ',', $qsParams['attributes'] );
        }

        // if we don't have attributes
        if( !is_array( $attributes ) || empty( $attributes ) )
        {
            // bail
            return array();
        }

        // add the root category id if it's not in there as we need it for getting the ecommerce links
        if( !in_array( 'root_category_id', $attributes ) )
        {
            $attributes[] = 'root_category_id';
        }

        // if we want a verbal description we need to get the standard description first.
        if( in_array( 'product_verbal_description', $attributes ) && !in_array( 'product_description', $attributes ) )
        {
            $removeDescription = true;
            $attributes[] = 'product_description';
        }

        // get the cached attributes
        $cachedObject = ( $resetCache ? array() : $this->getCachedObject( 'product_elements', 'product_id', $productId, $attributes, $resetCache ) );
        $this->deltaTimer( 'getCachedObject' );

        // if we got nothing back from cache or root_category_id was requested and it's not in the cache object
        if( empty( $cachedObject ) || ( in_array( 'root_category_id', $attributes ) && !isset( $cachedObject['root_category_id'] ) ) )
        {
            // get the product details listing
            $product = $this->getProductsByProductId( $productId, $resetCache );
            $this->deltaTimer( 'getProductsByProductId' );

            // if we have a product
            if( !empty( $product ) )
            {
                // run through the set of missing elements
                foreach( $attributes as $missingElement )
                {
                    // get the element from the product
                    $productElement = $this->getProductElement( $product, $missingElement );

                    // if we have the element
                    if( !empty( $productElement ) )
                    {
                        // put it into the update set
                        $cachedObject[$missingElement] = $productElement;
                    }
                }
            }
        }

        if( in_array( 'product_verbal_description', $attributes ) && isset( $cachedObject['product_description'] ) )
        {
            // create the verbal description and if necessary discard the full description
            $textService = new TextService();
            $cachedObject['product_verbal_description'] = $textService->createVerbalDescription( $cachedObject['product_description'] );
            if( isset( $removeDescription ) )
            {
                unset( $cachedObject['product_description'] );
            }
        }

        // need to check the cached data for related_items as some items may have been duplicated.
        if( isset( $cachedObject['related_items'] ) )
        {
            // create badItems flag for items which need re-caching due to formatting issues
            $badItems = false;
            $outOfDateItems = false;

            // if the result is not an array
            if( !is_array( $cachedObject['related_items'] ) )
            {
                // decode the JSON
                $cachedObject['related_items'] = json_decode( $cachedObject['related_items'], true );

                // set it for re-caching
                $badItems = true;
            }
            else
            {
                // if we're missing product_num_ratings or product_num_reviews
                if( !isset( $cachedObject['related_items'][0]['product_num_ratings'] ) || !isset( $cachedObject['related_items'][0]['product_num_ratings'] ) )
                {
                    // get related products for this product
                    $cachedObject['related_items'] = $this->getRelatedItems( $productId, $resetCache );

                    // else it's out of date so set the flag and skip it
                    $outOfDateItems = true;
                }
            }

            // run through the set of related items
            $checkedRelatedItems = array();
            $duplicateItems = false;
            foreach( $cachedObject['related_items'] as $relatedItem )
            {
                // if it's a valid object
                if( isset( $relatedItem['product_name'] ) )
                {
                    // if we don't already have the item
                    if( !isset( $checkedRelatedItems[$relatedItem['product_name']] ) )
                    {
                        // set it in our check array
                        $checkedRelatedItems[$relatedItem['product_name']] = $relatedItem;
                    }
                    else
                    {
                        // else it's duplicate so set the flag and skip it
                        $duplicateItems = true;
                    }

                }
                else
                {
                    // set it as a badly cached item
                    $badItems = true;
                }
            }

            // if we have duplicate or bad items
            if( $duplicateItems || $badItems || $outOfDateItems )
            {
                // update the cache indirectly
                $this->debugMessage( "Duplicate or Bad RelatedItem detected: queue $productId to product cache queue" );
                MessageQueue::create()
                            ->queueMessage( self::$_PRODUCT_CACHE_QUEUE_NAME, $productId, true );

                // replace the cached object set with the de-duplicated set
                $cachedObject['related_items'] = array_values( $checkedRelatedItems );
            }
        }

        // add ecommerce links to array if they're requested and not already cached
        $productEcommerceLinks = null;
        if( in_array( 'ecommerce_links', $attributes ) )
        {
            if( !isset( $cachedObject['ecommerce_links'] ) || $resetCache )
            {
                $productEcommerceLinks = $this->getProductEcommerceLinks( $productId, $cachedObject['root_category_id'] );
                if( $productEcommerceLinks )
                {
                    $cachedObject['ecommerce_links'] = $productEcommerceLinks['ecommerce_links'];
                }
            }
            // we may have an issue with logos for cached data pointing at the wrong server, so we're going to examine and fix that data
            if( isset( $cachedObject['ecommerce_links'] ) )
            {
                $itcherImageServerURI = YiiItcher::app()->params['imageServerURI'];
                $itcherImageServerURIBits = explode( '/', $itcherImageServerURI );
                foreach( $cachedObject['ecommerce_links'] as $countryCode => $countryData )
                {
                    for( $linkNum = 0, $numLinks = count( $countryData ); $linkNum < $numLinks; $linkNum++ )
                    {
                        if( isset( $countryData[$linkNum]['ecommerce_provider_logo_url'] ) )
                        {
                            // split the URL into bits
                            $urlBits = explode( '/', $countryData[$linkNum]['ecommerce_provider_logo_url'] );

                            // if it's an itcher base url and it's not correct
                            if( isset( $urlBits[2] ) && isset( $itcherBaseURIBits[2] ) && strpos( $urlBits[2], 'itcher' ) !== false && $urlBits[2] != $itcherBaseURIBits[2] )
                            {
                                // substitute in the itcher base URL
                                $cachedObject['ecommerce_links'][$countryCode][$linkNum]['ecommerce_provider_logo_url'] = str_replace( $urlBits[2], $itcherImageServerURIBits[2], $countryData[$linkNum]['ecommerce_provider_logo_url'] );
                            }
                        }
                    }
                }

            }
        }

        // add product ecommerce links to array if they're requested and not already cached
        if( in_array( 'ecommerce_product_links', $attributes ) )
        {
            if( !isset( $cachedObject['ecommerce_product_links'] ) || $resetCache )
            {
                $productEcommerceLinks = $this->getProductEcommerceLinks( $productId, $cachedObject['root_category_id'] );
                if( $productEcommerceLinks )
                {
                    $cachedObject['ecommerce_product_links'] = $productEcommerceLinks['ecommerce_product_links'];
                }
            }

            // if we have the object to return
            if( isset( $cachedObject['ecommerce_product_links'] ) )
            {
                // we may need to re-process image assets as they may be pointing at the wrong internal server or an external server and need to be cached
                $itcherImageServerURI = YiiItcher::app()->params['imageServerURI'];
                $itcherImageServerURIBits = explode( '/', $itcherImageServerURI );
                foreach( $cachedObject['ecommerce_product_links'] as $countryCode => $countryData )
                {
                    foreach( $countryData as $buyWatchLabel => $buyWatchData )
                    {
                        foreach( $buyWatchData as $ecommerceProvider => $ecommerceProviderLinks )
                        {
                            for( $linkNum = 0, $numLinks = count( $ecommerceProviderLinks ); $linkNum < $numLinks; $linkNum++ )
                            {
                                // we may have an issue with logos for cached data pointing at the wrong server, so we're going to examine and fix that data
                                if( isset( $ecommerceProviderLinks[$linkNum]['ecommerce_product_provider_logo_url'] ) )
                                {
                                    // split the URL into bits
                                    $urlBits = explode( '/', $ecommerceProviderLinks[$linkNum]['ecommerce_product_provider_logo_url'] );

                                    // if it's an itcher base url and it's not correct
                                    if( isset( $urlBits[2] ) && isset( $itcherImageServerURIBits[2] ) && strpos( $urlBits[2], 'itcher' ) !== false && $urlBits[2] != $itcherImageServerURIBits[2] )
                                    {
                                        // substitute in the itcher base URL
                                        $cachedObject['ecommerce_product_links'][$countryCode][$buyWatchLabel][$ecommerceProvider][$linkNum]['ecommerce_product_provider_logo_url'] = str_replace( $urlBits[2], $itcherImageServerURIBits[2], $ecommerceProviderLinks[$linkNum]['ecommerce_product_provider_logo_url'] );
                                    }
                                }
                                // we may have an issue with image resources pointing at either the wrong internal server or an external server
                                if( isset( $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] ) )
                                {
                                    // split the URL into bits
                                    $urlBits = explode( '/', $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] );

                                    // if it's an itcher base url
                                    if( isset( $urlBits[2] ) && strpos( $urlBits[2], 'itcher' ) !== false )
                                    {
                                        // if it's not correct
                                        if( isset( $itcherImageServerURIBits[2] ) && $urlBits[2] != $itcherImageServerURIBits[2] )
                                        {
                                            // substitute in the itcher image server URL
                                            $cachedObject['ecommerce_product_links'][$countryCode][$buyWatchLabel][$ecommerceProvider][$linkNum]['ecommerce_product_image_url'] = str_replace( $urlBits[2], $itcherImageServerURIBits[2], $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] );
                                        }
                                    }
                                    // else
                                    else
                                    {
                                        // send back a link to the appropriate image server with the URLEncoded external link
                                        $cachedObject['ecommerce_product_links'][$countryCode][$buyWatchLabel][$ecommerceProvider][$linkNum]['ecommerce_product_image_url'] = $itcherImageServerURI . 'image/resize/width/240/height/360/uri/' . urlencode( urlencode( $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] ) );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // need to hack/process info for now as we don't have language constants defined
        if( isset( $cachedObject['info'] ) )
        {
            if( is_array( $cachedObject['info'] ) && !empty( $cachedObject['info'] ) )
            {
                // run through the info entries
                $revisedInfo = array();
                foreach( $cachedObject['info'] as $infoBit )
                {
                    // if there is a non-blank title
                    if( isset( $infoBit['title'] ) && !empty( $infoBit['title'] ) )
                    {
                        // if the value isn't an array, make it one
                        if( !is_array( $infoBit['value'] ) )
                        {
                            $infoBit['value'] = array($infoBit['value']);
                        }

                        // Title Case the attribute name and remove underscores
                        $revisedInfo[] = array("title" => Utilities::strToTitle( $infoBit['title'] ), 'value' => $infoBit['value']);
                    }
                }

                // if the revised info slug isn't empty
                if( !empty( $revisedInfo ) )
                {
                    // set it into the result
                    $cachedObject['info'] = $revisedInfo;
                }
                // else
                else
                {
                    // get rid of it from the result
                    unset( $cachedObject['info'] );
                }
            }
            else
            {
                // get rid of it from the result
                unset( $cachedObject['info'] );
            }
        }

        // cleanup empty arrays
        foreach( $cachedObject as $key => $value )
        {
            if( is_array( $cachedObject[$key] ) && empty( $cachedObject[$key] ) )
            {
                unset( $cachedObject[$key] );
            }
        }

        $this->endTimer( 'getProductAttributesByProductId' );

        // return the result
        return $cachedObject;
    }

    /**
     * Get the list of products with a productId
     *
     * @param integer $productId
     * @param bool    $resetCache
     *
     * @return array|null
     */
    public function getProductsByProductId( $productId, $resetCache = false )
    {
        /*
         * UK only to start
         */
        $countryCode = 'GB';
        $timeNow = floor( microtime( true ) );

        $this->startTimer( 'getProductsByProductId' );

        $product = null;
        /**
         * @var $productModel Product
         */
        $productModel = Product::model()
                               ->findByPk( $productId );
        $this->deltaTimer( 'getModel' );
        if( $productModel )
        {
            // get rid of the product attributes
            $product = $productModel->getAttributes();
            $this->deltaTimer( 'getAttributes' );

            // get rid of any attributes we don't want to publish
            $this->discardUnwantedAttributes( $product );
            $this->deltaTimer( 'discardUnwantedAttributes' );

            //format release_date
            $product['release_date'] = $this->validateReleaseDate( $product['release_date'] );

            // if the item is a sub-product
            $parentProduct = null;
            if( $product['is_subproduct'] )
            {
                // get the parent product
                $parentProduct = $productModel->getParentProduct();
            }

            // update the product_average_rating to add half a point if non-zero as it's difficult to find highly weighted averages
            $product['product_average_rating'] = strval( $product['product_average_rating'] != 0 ? min( 5, ( $product['product_average_rating'] + 0.5 ) ) : 0 );

            // update the product_weighted_rating to 0 if it's null otherwise it'll continually be re-queued for cache update
            $product['product_average_rating'] = is_null( $product['product_average_rating'] ) ? 0 : $product['product_average_rating'];

            // get the first category for the product
            if( !$product['root_category_id'] = $productModel->getRootCategoryId() )
            {
                // if we have a parent product
                if( $parentProduct )
                {
                    // get the parent product root category ID
                    $product['root_category_id'] = $parentProduct['root_category_id'];
                }

                // bail if we don't have a category
                if( $product['root_category_id'] == -1 )
                {
                    return null;
                }
            }

            // get product images
            $productImages = $this->getAndFilterProductImages( $product );
            if( $productImages )
            {
                // add product images to array
                $product['product_attributes']['all_images'] = $productImages;
            }
            $this->deltaTimer( 'getProductImages' );

            // add product categories to array
            $productCategories = $this->getProductCategories( $productId, $resetCache );
            if( $productCategories )
            {
                $product['categories'] = $productCategories;
            }
            $this->deltaTimer( 'getProductCategories' );

            // add product ecommerce links to array
            $productEcommerceLinks = $this->getProductEcommerceLinks( $productId, $product['root_category_id'] );
            if( $productEcommerceLinks )
            {
                // we may have an issue with logos for cached data pointing at the wrong server, so we're going to examine and fix that data
                $itcherImageServerURI = YiiItcher::app()->params['imageServerURI'];
                $itcherImageServerURIBits = explode( '/', $itcherImageServerURI );

                // re-process old style links for caching
                if( isset( $productEcommerceLinks['ecommerce_links'] ) )
                {
                    foreach( $productEcommerceLinks['ecommerce_links'] as $countryCode => $countryData )
                    {
                        for( $linkNum = 0, $numLinks = count( $countryData ); $linkNum < $numLinks; $linkNum++ )
                        {
                            if( isset( $countryData[$linkNum]['ecommerce_provider_logo_url'] ) )
                            {
                                // split the URL into bits
                                $urlBits = explode( '/', $countryData[$linkNum]['ecommerce_provider_logo_url'] );

                                // if it's an itcher base url and it's not correct
                                if( isset( $urlBits[2] ) && isset( $itcherBaseURIBits[2] ) && strpos( $urlBits[2], 'itcher' ) !== false && $urlBits[2] != $itcherBaseURIBits[2] )
                                {
                                    // substitute in the itcher base URL
                                    $productEcommerceLinks['ecommerce_links'][$countryCode][$linkNum]['ecommerce_provider_logo_url'] = str_replace( $urlBits[2], $itcherImageServerURIBits[2], $countryData[$linkNum]['ecommerce_provider_logo_url'] );
                                }
                            }
                        }
                    }

                    // add to the product attributes
                    $product['product_attributes']['ecommerce_links'] = $productEcommerceLinks['ecommerce_links'];
                }

                // re-process new style links for caching
                if( isset( $productEcommerceLinks['ecommerce_product_links'] ) )
                {
                    foreach( $productEcommerceLinks['ecommerce_product_links'] as $countryCode => $countryData )
                    {
                        foreach( $countryData as $buyWatchLabel => $buyWatchData )
                        {
                            foreach( $buyWatchData as $ecommerceProvider => $ecommerceProviderLinks )
                            {
                                for( $linkNum = 0, $numLinks = count( $ecommerceProviderLinks ); $linkNum < $numLinks; $linkNum++ )
                                {
                                    // we may have an issue with logos for cached data pointing at the wrong server, so we're going to examine and fix that data
                                    if( isset( $ecommerceProviderLinks[$linkNum]['ecommerce_product_provider_logo_url'] ) )
                                    {
                                        // split the URL into bits
                                        $urlBits = explode( '/', $ecommerceProviderLinks[$linkNum]['ecommerce_product_provider_logo_url'] );

                                        // if it's an itcher base url and it's not correct
                                        if( isset( $urlBits[2] ) && isset( $itcherImageServerURIBits[2] ) && strpos( $urlBits[2], 'itcher' ) !== false && $urlBits[2] != $itcherImageServerURIBits[2] )
                                        {
                                            // substitute in the itcher base URL
                                            $productEcommerceLinks['ecommerce_product_links'][$countryCode][$buyWatchLabel][$ecommerceProvider][$linkNum]['ecommerce_product_provider_logo_url'] = str_replace( $urlBits[2], $itcherImageServerURIBits[2], $ecommerceProviderLinks[$linkNum]['ecommerce_product_provider_logo_url'] );
                                        }
                                    }
                                    // we may have an issue with image resources pointing at either the wrong internal server or an external server
                                    if( isset( $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] ) )
                                    {
                                        // split the URL into bits
                                        $urlBits = explode( '/', $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] );

                                        // if it's an itcher base url
                                        if( isset( $urlBits[2] ) && strpos( $urlBits[2], 'itcher' ) !== false )
                                        {
                                            // if it's not correct
                                            if( isset( $itcherImageServerURIBits[2] ) && $urlBits[2] != $itcherImageServerURIBits[2] )
                                            {
                                                // substitute in the itcher image server URL
                                                $productEcommerceLinks['ecommerce_product_links'][$countryCode][$buyWatchLabel][$ecommerceProvider][$linkNum]['ecommerce_product_image_url'] = str_replace( $urlBits[2], $itcherImageServerURIBits[2], $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] );
                                            }
                                        }
                                        // else
                                        else
                                        {
                                            // send back a link to the appropriate image server with the URLEncoded external link
                                            $productEcommerceLinks['ecommerce_product_links'][$countryCode][$buyWatchLabel][$ecommerceProvider][$linkNum]['ecommerce_product_image_url'] = $itcherImageServerURI . 'image/resize/width/240/height/360/uri/' . urlencode( urlencode( $ecommerceProviderLinks[$linkNum]['ecommerce_product_image_url'] ) );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // add to the product attributes
                    $product['product_attributes']['ecommerce_product_links'] = $productEcommerceLinks['ecommerce_product_links'];
                }

            }
            $this->deltaTimer( 'getProductEcommerceLinks' );

            // add product attributes
            $this->getProductAttributes( $product, true, $resetCache );
            $this->deltaTimer( 'product_attributes' );

            // get related products for this product - a full reset for the parent does not provide a full reset for all related products, just a pre-cache call
            $relatedItems = $this->getRelatedItems( $productId, false, $resetCache );
            if( $relatedItems )
            {
                // add to the product attributes
                $product['product_attributes']['related_items'] = $relatedItems;
            }
            $this->deltaTimer( 'getRelatedItems' );

            // if the product hasn't been updated for 7 days
            if( $productModel->last_updated < ( $timeNow - self::RELATED_CACHE_TIME ) )
            {
                // if we have one
                if( $product['root_category_id'] != -1 )
                {
                    // queue fetch for related products
                    $this->queueRelatedProducts( $countryCode, array(
                            'category_id' => $productModel->getRootCategoryId(),
                            'product_id'  => $productModel->product_id
                        ) );
                }
            }
            $this->deltaTimer( 'queueRelatedProducts' );

            // if this product is a sub product then list other sub products this belongs to
            if( $parentProduct )
            {
                // get parent product
                $product['parent_product'] = null;

                // get rid of any attributes we don't want to publish
                $this->discardUnwantedAttributes( $parentProduct );

                // get product images
                $this->getAndFilterProductImages( $parentProduct );
                $product['parent_product'] = $parentProduct;

                // get other products
                $product['other_subproducts'] = array();
                $otherProductsSQL = "
                        SELECT p.*, pst.product_subproduct_type_name
                        FROM product_subproduct ps
                        JOIN product p ON (p.product_id = ps.subproduct_id)
                        JOIN product_subproduct_type pst ON (pst.product_subproduct_type_id = ps.product_subproduct_type_id)
                        JOIN product_subproduct ps2 ON (ps2.product_id = ps.product_id)
                        WHERE ps2.subproduct_id = :product_id AND p.product_id != :product_id AND p.display = 1
                        ORDER BY p.release_date DESC
                    ";
                $otherProducts = $this->_db->createCommand( $otherProductsSQL )
                                           ->queryAll( true, array(
                                                   ':product_id' => $productId
                                               ) );
                if( $otherProducts )
                {
                    foreach( $otherProducts as $otherProduct )
                    {
                        // get rid of any attributes we don't want to publish
                        $this->discardUnwantedAttributes( $otherProduct );

                        // subproducts don't sit in categories, so pass the parent root_category_id
                        $otherProduct['root_category_id'] = $product['root_category_id'];

                        //format release_date
                        $otherProduct['release_date'] = $this->validateReleaseDate( $otherProduct['release_date'] );

                        // get product images for the product and filter them for primary and other images
                        $this->getAndFilterProductImages( $otherProduct, false );

                        // assign to the subproduct
                        $product['other_subproducts'][$otherProduct['product_subproduct_type_name']][] = $otherProduct;
                    }
                }
            }

            //  Update the product description from available extra information and get subproducts with images
            $this->updateProductTitleDescriptionSubproducts( $product, true, true, true );

            // remove the extra formatting put into the description for linkage
            $product['product_short_description'] = mb_substr( preg_replace( "/\[iLink name='([^']+)' id='\d+'\]/", "$1", str_replace( '[iHl]', ' ', str_replace( '[/iHl]', ': ', $product['product_description'] ) ) ), 0, 512, "UTF-8" );
            $this->deltaTimer( 'product_subproducts' );
        }

        // if we filled in a product
        if( !empty( $product ) )
        {
            // if we don't have a product_short_description but we do have a product_description
            if( !isset( $product['product_short_description'] ) && isset( $product['product_description'] ) )
            {
                $product['product_short_description'] = mb_substr( preg_replace( "/\[iLink name='([^']+)' id='\d+'\]/", "$1", str_replace( '<br>', ' ', str_replace( '[iHl]', ' ', str_replace( '[/iHl]', ': ', $product['product_description'] ) ) ) ), 0, 512, "UTF-8" );
            }

            // if we have a rotten tomatoes URI set it as a string
            $rottenTomatoesURI = null;
            if( isset( $product['product_attributes']['rotten_tomatoes_uri'] ) )
            {
                if( is_array( $product['product_attributes']['rotten_tomatoes_uri'] ) )
                {
                    $rottenTomatoesURI = $product['product_attributes']['rotten_tomatoes_uri'][0];
                }
                else
                {
                    $rottenTomatoesURI = $product['product_attributes']['rotten_tomatoes_uri'];
                }
            }

            if( empty( $product['last_indexed'] || $product{'last_updated'} > $product['last_indexed'] ) )
            {
                MessageQueue::create()
                            ->queueMessage( self::SEARCH_ADD_QUEUE_NAME, $product['product_id'] );
            }

            // make sure the product has a primary square image URL
            $this->getProductSquareImage( $product );

            // generate the sub-set needed for listings, thumbnails, search hints
            $productListingElements = array(
                'product_id'                       => $product['product_id'],
                'product_name'                     => $product['product_name'],
                'product_average_rating'           => $product['product_average_rating'],
                'product_num_ratings'              => $product['product_num_ratings'],
                'product_num_reviews'              => $product['product_num_reviews'],
                'product_short_description'        => $product['product_short_description'],
                'product_short_description_length' => strlen( $product['product_short_description'] ),
                'product_description'              => $product['product_description'],
                'product_description_length' => strlen( $product['product_description'] ),
                'root_category_id'           => $product['root_category_id'],
                'product_weighted_rating'    => $product['product_weighted_rating'],
                'is_subproduct'              => $product['is_subproduct'],
                'release_date'               => $product['release_date'],
                'product_sort_name'          => $product['product_sort_name'],
                'primary_portrait_image'     => isset( $product['images']['primary_portrait_image']['image_url'] ) ? $product['images']['primary_portrait_image']['image_url'] : null,
                'primary_landscape_image'    => isset( $product['images']['primary_landscape_image']['image_url'] ) ? $product['images']['primary_landscape_image']['image_url'] : null,
                'square_image_url'           => isset( $product['square_image_url'] ) ? $product['square_image_url'] : null,
                'categories'                 => isset( $product['categories'] ) ? $product['categories'] : array(),
                'images'                     => isset( $product['images'] ) ? $product['images'] : array(),
                'all_images'                 => isset( $product['product_attributes']['all_images'] ) ? $product['product_attributes']['all_images'] : array(),
                'cast'                       => isset( $product['product_attributes']['cast'] ) ? $product['product_attributes']['cast'] : array(),
                'crew'                       => isset( $product['product_attributes']['crew'] ) ? $product['product_attributes']['crew'] : array(),
                'info'                       => isset( $product['product_attributes']['info'] ) ? $product['product_attributes']['info'] : array(),
                'trailers'                   => isset( $product['product_attributes']['trailers'] ) ? $product['product_attributes']['trailers'] : array(),
                'video_reviews'              => isset( $product['product_attributes']['video_reviews'] ) ? $product['product_attributes']['video_reviews'] : array(),
                'editorial_reviews'          => isset( $product['product_attributes']['editorial_reviews'] ) ? $product['product_attributes']['editorial_reviews'] : array(),
                'music_videos'               => isset( $product['product_attributes']['music_videos'] ) ? $product['product_attributes']['music_videos'] : array(),
                'news'                       => isset( $product['product_attributes']['news'] ) ? $product['product_attributes']['news'] : array(),
                'related_items'              => isset( $product['product_attributes']['related_items'] ) ? $product['product_attributes']['related_items'] : array(),
                'spotify_uri'                => isset( $product['product_attributes']['spotify_uri'] ) ? $product['product_attributes']['spotify_uri'] : array(),
                'rotten_tomatoes_uri'        => $rottenTomatoesURI,
                'ecommerce_links'            => isset( $product['product_attributes']['ecommerce_links'] ) ? $product['product_attributes']['ecommerce_links'] : array(),
                'ecommerce_product_links'    => isset( $product['product_attributes']['ecommerce_product_links'] ) ? $product['product_attributes']['ecommerce_product_links'] : array(),
                'urls'                       => isset( $product['product_attributes']['urls'] ) ? $product['product_attributes']['urls'] : array(),
                // 'wikipedia'                        => isset( $product['product_attributes']['wikipedia'] ) ? $product['product_attributes']['wikipedia'] : array(),
                'music_album'                => isset( $product['product_attributes']['music_album'] ) ? $product['product_attributes']['music_album'] : array(),
                'music_single'               => isset( $product['product_attributes']['music_single'] ) ? $product['product_attributes']['music_single'] : array(),
                'tv_season'                  => isset( $product['product_attributes']['tv_season'] ) ? $product['product_attributes']['tv_season'] : array(),
                'tv_episode'                 => isset( $product['product_attributes']['tv_episode'] ) ? $product['product_attributes']['tv_episode'] : array(),
                'publisher'                  => isset( $product['product_attributes']['publisher'] ) ? $product['product_attributes']['publisher'] : array(),
                'platform'                   => isset( $product['product_attributes']['urls'] ) ? $product['product_attributes']['urls'] : array(),
                'genre'                      => isset( $product['product_attributes']['genre'] ) ? $product['product_attributes']['genre'] : array(),
                'developer'                  => isset( $product['product_attributes']['developer'] ) ? $product['product_attributes']['developer'] : array(),
                'author'                     => isset( $product['product_attributes']['author'] ) ? $product['product_attributes']['author'] : array(),
                'artist'                     => isset( $product['product_attributes']['artist'] ) ? $product['product_attributes']['artist'] : array(),
            );

            // if we're doing a forced cache reset
            if( $resetCache )
            {
                // update the cache directly
                $this->setCachedObjectQueued( 'product_elements', 'product_id', $productId, $productListingElements, false );
            }
            else
            {
                // update the cache indirectly
                $this->debugMessage( "Queue $productId to product cache queue" );
                MessageQueue::create()
                            ->queueMessage( self::$_PRODUCT_CACHE_QUEUE_NAME, $productId, true );

            }

            // update the product record to just contain the listing elements
            $product = $productListingElements;

        }

        $this->deltaTimer( 'done' );

        return $product;
    }

    /**
     * Get the set of product attributes for the passed product
     *
     * @param array $product
     * @param bool  $getCastDetails - flag to say whether to generate full cast details
     * @param bool  $resetCache
     */
    public function getProductAttributes( &$product, $getCastDetails = true, $resetCache = false )
    {
        // get the cached set of product attribute types if we don't have it already
        if( is_null( $this->_productAttributeTypes ) )
        {
            $this->_productAttributeTypes = $this->cacheGet( 'product_attribute_types' );
            if( is_null( $this->_productAttributeTypes ) )
            {
                // if we don't have it cached then set it as a blank array
                $this->_productAttributeTypes = array();
            }
        }

        /**
         * get the set of product attributes for this product
         *
         *
         * @var $productAttributes ProductAttribute
         */
        $productAttributes = ProductAttribute::model()
                                             ->findAllByAttributes( array(
                                                 'product_id' => $product['product_id']
                                             ) );

        // create a new object if we don't already have one
        if( isset( $product['product_attributes'] ) )
        {
            $productAttributesArray = $product['product_attributes'];
        }
        else
        {
            $productAttributesArray = array();
        }

        // pull in the indexed attributes
        foreach( $productAttributes as $productAttribute )
        {
            // we already have the attribute cached
            if( isset( $this->_productAttributeTypes[$productAttribute['product_attribute_type_id']] ) )
            {
                // get it
                $productAttributeTypeData = $this->_productAttributeTypes[$productAttribute['product_attribute_type_id']];
            }
            // else
            else
            {
                // get it from the database
                $productAttributeType = ProductAttributeType::model()
                                                            ->findByPk( $productAttribute['product_attribute_type_id'] );
                $productAttributeTypeData = $productAttributeType->getAttributes();
                $this->_productAttributeTypes[$productAttribute['product_attribute_type_id']] = $productAttributeTypeData;

                // cache it
                $this->cacheSet( 'product_attribute_types', $this->_productAttributeTypes, self::$_CACHE_TIME_ONE_DAY );
            }

            // if we have a name and a value, and a productAttributeType
            if( $productAttributeTypeData )
            {
                // set the name and ID
                $productAttributeTypeName = Utilities::fromCamelCase( $productAttributeTypeData['product_attribute_type_name'] );
                $productAttributeValue = $productAttribute->product_attribute_value;

                // assume anything starting with a brace is some json encoded data ( i don't like this method, but the
                // alternative is to start storing hierarchical tuples )
                // this is a temporary trade off
                if( $productAttributeValue[0] == '{' || $productAttributeValue[0] == '[' )
                {
                    $attributeElements = json_decode( $productAttributeValue, true );
                    switch( $productAttributeTypeName )
                    {
                        case 'cast':
                            // if we want to generate full cast details
                            if( $getCastDetails )
                            {
                                // run through the set of actors
                                $cast = array();
                                foreach( $attributeElements as $attributeElement )
                                {
                                    // get the detailed information
                                    $actorInfo = $this->getActorInformation( $attributeElement['name'], $resetCache, $product['product_id'] );

                                    // get the short bio
                                    $shortBio = $this->substrILink( $actorInfo['product_description'], 500 );

                                    // get the character
                                    $character = '';
                                    if( isset( $attributeElement['character'] ) )
                                    {
                                        $character = $attributeElement['character'];
                                    }

                                    // add to the cast list
                                    $cast[] = array(
                                        'who'       => $attributeElement['name'],
                                        'what'      => $character,
                                        'image'     => $actorInfo['image_url'],
                                        'bio'       => $actorInfo['product_description'],
                                        'short_bio' => $shortBio
                                    );
                                }
                                $productAttributesArray[$productAttributeTypeName] = $cast;
                            }
                            break;
                        case 'crew':
                            // if we want to generate full crew details
                            if( $getCastDetails )
                            {
                                // run through the set of crew
                                $cast = array();
                                $castImages = array();
                                foreach( $attributeElements as $attributeElement )
                                {
                                    // get the detailed information
                                    $actorInfo = $this->getActorInformation( $attributeElement['name'], $resetCache );

                                    // get the short bio
                                    $shortBio = $this->substrILink( $actorInfo['product_description'], 500 );

                                    $cast[] = array(
                                        'who'       => $attributeElement['name'],
                                        'what'      => $attributeElement['job'],
                                        'image'     => $actorInfo['image_url'],
                                        'bio'       => $actorInfo['product_description'],
                                        'short_bio' => $shortBio
                                    );
                                    $castImages[] = $actorInfo['image_url'];
                                }

                                // sort by the items with images first
                                array_multisort( $castImages, SORT_DESC, $cast );
                                $productAttributesArray[$productAttributeTypeName] = $cast;
                            }
                            break;
                        case 'tracks':
                            $productAttributesArray[$productAttributeTypeName] = $attributeElements;
                            break;
                        case 'urls':
                            //if te attribute is urls parse them to match 'provider:, link:' array
                            $returnUrlArray = array();
                            $index = 0;
                            foreach( $attributeElements as $key => $value )
                            {
                                $returnUrlArray[$index]['provider'] = $key;
                                $returnUrlArray[$index]['link'] = $value;
                                $index++;

                            }
                            $productAttributesArray[$productAttributeTypeName] = $returnUrlArray;
                            break;
                        case 'trailers':
                        case 'video_reviews':
                            //check to see how the array from json string is indexed
                            $keys = array_keys( $attributeElements );

                            //if the array is indexed by a source string(youtube for example leave it as is)
                            if( is_array( $keys ) && isset( $keys[0] ) && is_string( $keys[0] ) )
                            {
                                $orderedArray = $attributeElements;
                            }
                            //if the indexes are numerical sort the array to index by source
                            else
                            {
                                $orderedArray = array();
                                foreach( $attributeElements as $attributeElement )
                                {
                                    if( is_array( $attributeElement ) )
                                    {
                                        $keyValue = array_keys( $attributeElement );
                                        if( is_array( $keyValue ) && isset( $keyValue[0] ) )
                                        {

                                            $orderedArray[$keyValue[0]][] = $attributeElement[$keyValue[0]];
                                        }
                                        else
                                        {
                                            $orderedArray = $attributeElement;
                                        }
                                    }
                                    else
                                    {
                                        $orderedArray = $attributeElement;
                                    }
                                }

                            }
                            $attributeElements = $orderedArray;
                            $productAttributesArray[$productAttributeTypeName] = $attributeElements;
                            break;
                        default:
                            // if the items are not already in a 0-based array put them in one
                            if( isset( $attributeElements[0] ) )
                            {
                                $productAttributesArray[$productAttributeTypeName] = $attributeElements;
                            }
                            else
                            {
                                $productAttributesArray[$productAttributeTypeName] = array($attributeElements);
                            }
                            break;
                    }
                }
                else
                {
                    if( is_array( $productAttributeValue ) )
                    {
                        $productAttributesArray[$productAttributeTypeName] = $productAttributeValue;
                    }
                    else
                    {
                        $productAttributesArray[$productAttributeTypeName] = array($productAttributeValue);
                    }
                }
            }
        }

        // sort product attributes by key
        ksort( $productAttributesArray );

        // return the product attributes
        $product['product_attributes'] = $productAttributesArray;
    }

    /**
     * Get related products for this product
     *
     * @param integer $productId
     *
     * @param bool    $resetCache       - do full cache reset
     * @param bool    $precacheProducts - precache product, i.e. do not re-write to LRU
     *
     * @return array related items
     */
    public function getRelatedItems( $productId, $resetCache = false, $precacheProducts = false )
    {
        $relatedItems = array();
        // do a similar items lookup
        $similarProductsSQL = '
          SELECT * FROM
          (
            SELECT
              p.product_id, p.product_name_md5, similarity_score, product_weighted_rating
            FROM
              product_similar_product psp
              JOIN product p
                ON
                  p.product_id = psp.similar_product_id
              JOIN product_image
                ON product_image.product_id = p.product_id
            WHERE psp.product_id = :product_id
              AND p.display = 1
              AND p.product_root_category_id IN (' . $this->getRootCategoriesCSV( 'GB' ) . ')
          ) related_products
          GROUP BY product_name_md5
          ORDER BY similarity_score * product_weighted_rating DESC';
        $relatedProducts = $this->_db->createCommand( $similarProductsSQL )
                                     ->queryAll( true, array(
                                             ':product_id' => $productId,
                                         ) );

        if( empty( $relatedProducts ) )
        {
            // do a related items lookup
            $relatedProductsSQL = '
          SELECT * FROM
          (
            SELECT
              p.product_id, p.product_name_md5, relevancy_score, product_weighted_rating
            FROM
              related_product rp
              JOIN product p
                ON
                  p.product_id = rp.related_product_id
              JOIN product_image
                ON product_image.product_id = p.product_id
            WHERE rp.product_id = :product_id AND rp.related_product_id != :product_id
              AND p.display = 1
              AND p.product_root_category_id IN (' . $this->getRootCategoriesCSV( 'GB' ) . ')
            UNION
            SELECT
              p.product_id, p.product_name_md5, relevancy_score, product_weighted_rating
            FROM
              related_product rp
               JOIN product p
                ON
                  p.product_id = rp.product_id
              JOIN product_image
                ON product_image.product_id = p.product_id
            WHERE rp.related_product_id = :product_id AND rp.product_id != :product_id
              AND p.display = 1
              AND p.product_root_category_id IN (' . $this->getRootCategoriesCSV( 'GB' ) . ')

          ) related_products
          GROUP BY product_name_md5
          ORDER BY relevancy_score * product_weighted_rating DESC';
            $relatedProducts = $this->_db->createCommand( $relatedProductsSQL )
                                         ->queryAll( true, array(
                                                 ':product_id' => $productId,
                                             ) );

            // todo: queue for similar items (need to implement the queue first)
        }
        // if we have some
        if( $relatedProducts )
        {
            // run through the products returned
            foreach( $relatedProducts as $relatedProduct )
            {
                // pre-define the image
                $image = null;

                // get what we need for related products
                $requiredElements = array(
                    'product_name',
                    'images',
                    'square_image_url',
                    'product_average_rating',
                    'product_weighted_rating',
                    'product_num_ratings',
                    'product_num_reviews',
                    'categories',
                    'ecommerce_product_links',
                );
                $this->getProductElementsThroughCache( $relatedProduct, $requiredElements, $resetCache, $precacheProducts );

                // assign to the array
                if( !empty( $relatedProduct ) )
                {
                    $relatedItems[] = $relatedProduct;
                }

                // if we have enough items
                if( count( $relatedItems ) >= 8 )
                {
                    // bail
                    break;
                }
            }
        }

        // and return the results
        return $relatedItems;
    }

    /**
     * Get the set of additional elements for a product through the cache
     *
     * @param array $product          product to be updated
     * @param array $elements         elements to be updated
     * @param bool  $resetCache
     * @param bool  $precacheProducts - don't constantly flush the LRU if we're just pre-caching as we ony want it in Dynamo
     *
     * @throws CException
     * @throws Exception
     */
    public function getProductElementsThroughCache( &$product, $elements, $resetCache = false, $precacheProducts = false )
    {
        // placeholder for images should we need primary images and / or all images
        $images = null;

        // define the set of needed additional elements
        $additionalElements = array('product_name', 'root_category_id', 'release_date', 'product_id', 'is_subproduct');

        // run through the list of additional elements
        foreach( $additionalElements as $key )
        {
            // if the additional element isn't in the list of elements, add it, as it's a pre-requisite for other items
            if( !in_array( $key, $elements ) )
            {
                $elements[] = $key;
            }
        }
        // get the cached elements
        if( $resetCache )
        {
            $cachedObject = array();
        }
        else
        {
            $cachedObject = $this->getCachedObject( 'product_elements', 'product_id', $product['product_id'], $elements, $precacheProducts );
        }

        // set a blank cache update array
        $cacheUpdates = array();

        // run through them
        foreach( $elements as $key )
        {
            if( isset( $cachedObject[$key] ) )
            {
                // if we don't have a result
                if( empty( $cachedObject[$key] ) )
                {
                    if( !isset( $this->_productCacheFieldsAllowableZero[$key] ) || ( isset( $this->_productCacheFieldsAllowableZero[$key] ) && $cachedObject[$key] != 0 ) )
                    {
                        // set it needs updating
                        $cacheUpdates[$key] = 1;
                    }
                }
            }
            else
            {
                // set it needs updating
                $cachedObject[$key] = '';
                $cacheUpdates[$key] = 1;
            }
        }

        // if we have non-critical cache elements missing
        if( !empty( $cacheUpdates ) )
        {
            // run through the keys
            foreach( array_keys( $cacheUpdates ) as $updateKey )
            {
                // check the key
                switch( $updateKey )
                {
                    // check ecommerce_product_links
                    case 'ecommerce_product_links':
                        // if we don't have any EcommerceProviderProduct for the product
                        if( !$ecommerceProviderProduct = EcommerceProviderProduct::model()
                                                                                 ->findByAttributes( array('product_id' => $product['product_id']) )
                        )
                        {
                            // remove from cache updates
                            unset ( $cacheUpdates[$updateKey] );
                        }
                        break;

                    // check product_weighted_rating
                    case 'product_weighted_rating':
                        // if there's no product_weighted_rating it's cause it's NULL in the database, so set it to 0
                        $cachedObject[$updateKey] = 0;

                        // remove from cache updates
                        unset ( $cacheUpdates[$updateKey] );
                        break;
                    default:
                        break;
                }
            }

            // if we still have updates to process
            if( !empty( $cacheUpdates ) )
            {
                $this->debugMessage( "Queue for re-cacheing: " . json_encode( array_keys( $cacheUpdates ) ) );

                // queue the message for re-generation and re-caching
                MessageQueue::create()
                            ->queueMessage( self::$_PRODUCT_CACHE_QUEUE_NAME, $product['product_id'] );
            }
        }

        // if we need critical cache updates or been asked to reset
        if( isset( $cacheUpdates['product_name'] ) || isset( $cacheUpdates['product_short_description'] ) || isset( $cacheUpdates['release_date'] ) || $resetCache )
        {
            // get the top product which matches the product name and category
            $topProductSQL = "SELECT p.*, IF(product_average_rating != 0, LEAST(5, (product_average_rating + 0.5)), 0) AS product_average_rating, product_root_category_id AS root_category_id,
                IF(product_root_category_id IN (" . $this->getRootCategoriesCSV( 'GB' ) . "),1,0) AS valid_root_category_id, release_date
                FROM product p
                WHERE p.product_id = " . $product['product_id'] . "
                AND archived = 0
                AND display = 1
                LIMIT 1
                ";

            /**
             * get the product
             * @var $command CDbCommand
             */
            $command = $this->_db->createCommand( $topProductSQL );
            $product = $command->queryRow( true );

            // if we didn't get a product
            if( !$product )
            {
                return;
            }
        }

        // run through them
        foreach( $cachedObject as $key => $value )
        {
            // if we don't have a result
            if( ( empty( $value ) && !isset( $this->_productCacheFieldsAllowableZero[$key] ) ) || $resetCache )
            {
                // generate the required value
                switch( $key )
                {
                    case 'product_short_description':

                        //  Update the product description from available extra information and get subproducts with no images
                        $this->updateProductTitleDescriptionSubproducts( $product, false, false, false );

                        // remove the extra formatting put into the description for linkage
                        $product['product_short_description'] = mb_substr( preg_replace( "/\[iLink name='([^']+)' id='\d+'\]/", "$1", str_replace( '<br>', ' ', str_replace( '[iHl]', ' ', str_replace( '[/iHl]', ': ', $product['product_description'] ) ) ) ), 0, 512 );
                        break;

                    case 'categories':
                        // get the categories for the product
                        $product['categories'] = $this->getProductCategories( $product['product_id'] );
                        break;

                    case 'square_image_url':
                    case 'images':
                        // get the images data for the product
                        /*
                        $productImages = $this->getAndFilterProductImages( $product );
                        $product['product_attributes']['images'] = $productImages;
                        */
                        $this->getAndFilterProductImages( $product );
                        break;

                }

                // add it to the cache updates
                if( isset( $product[$key] ) )
                {
                    $cacheUpdates[$key] = $product[$key];
                }
            }
            else
            {
                // set it into the product
                $product[$key] = $value;
            }
        }

        // finally, run through the final product and remove all elements we didn't ask for
        $finalProduct = array();
        foreach( $elements as $key )
        {
            // if we've been asked for the product_short_description, save it as the product_description
            if( $key == 'product_short_description' )
            {
                $finalProduct['product_description'] = $product[$key];
            }
            else
            {
                if( isset( $product[$key] ) )
                {
                    $finalProduct[$key] = $product[$key];
                }
            }
        }

        // set the product to the final product
        $product = $finalProduct;
    }

    /**
     * Update product title and description (mostly for music) and get subproducts
     *
     * @param array $product
     * @param bool  $addHTMLTags
     * @param bool  $getImages
     * @param bool  $getCastDetails
     */
    public function updateProductTitleDescriptionSubproducts( &$product, $addHTMLTags = false, $getImages = false, $getCastDetails = false )
    {
        // if the item is a sub-product
        $parentProduct = null;
        if( $product['is_subproduct'] == 1 )
        {
            // get the parent product
            $parentProduct = Product::model()
                                    ->getParentProduct( $product['product_id'] );

            // add title of product if its a sub product (so we can get the artist - album)
            $product['product_name_original'] = $product['product_name'];
            $product['product_name'] = "{$parentProduct['product_name']} - {$product['product_name']}";
        }

        // get the product attributes
        $this->getProductAttributes( $product, $getCastDetails );

        // get sub products for this product with images if requested
        $this->getProductSubProducts( $product, $getImages );

        //  Update the product description from available extra information
        $this->updateProductDescription( $product, $addHTMLTags );
    }

    /**
     * Queue calls for related products for a specific product
     *
     * @param string $countryCode
     * @param array  $productData
     *
     * @internal param mixed $productName
     */
    private function queueRelatedProducts( $countryCode, $productData )
    {
        // if we haven't already queued this related product today
        $cacheKey = 'ProductAPI::queueRelatedProducts-' . $productData['product_id'];
        if( !$this->cacheGet( $cacheKey ) )
        {
            // set cached
            $this->cacheSet( $cacheKey, 1, self::RELATED_CACHE_TIME );

            // get the category for the product
            $categoryId = $productData['category_id'];

            // find the list of data providers for the category
            $dataProviders = $this->dataProvidersByCategoryByCountry( $countryCode, $categoryId );

            // if we have data providers
            if( count( $dataProviders ) )
            {
                // iterate through the list of data providers, calling the underlying models to get data
                foreach( $dataProviders as $dataProvider )
                {
                    // call the underlying model to fetch the data into the database
                    switch( $dataProvider['data_provider_name'] )
                    {
                        case AbstractDataProvider::AMAZON_DATA_PROVIDER_NAME :
                            // create a new AmazonDataProvider for this country to fetch data
                            $dataProvider = new AmazonDataProvider();
                            break;

                        case AbstractDataProvider::TMDB_DATA_PROVIDER_NAME :
                            // create a new TMDbProvider for this country to fetch data
                            $dataProvider = new TMDbDataProvider();
                            break;

                        default :
                            return;
                            break;
                    }

                    $dataProvider->init( $countryCode );

                    // the getRelatedProducts call will populate the related products in the database if they are our of date and
                    // need populating
                    $dataProvider->getRelatedProducts( array(
                        'product_id' => $productData['product_id']
                    ), true );
                }
            }
        }
        else
        {
            // echo( "Already queued today" );
        }
    }

    /**
     * Recursive function to find the element requested in the product array passed in.
     *
     * @param $productElements
     * @param $missingElement
     *
     * @return null | mixed
     */
    private function getProductElement( $productElements, $missingElement )
    {
        // placeholder for results
        $missingElementValue = null;

        // if the missing element is in this level of the array
        if( isset( $productElements[$missingElement] ) )
        {
            // return the value
            $missingElementValue = $productElements[$missingElement];
        }
        else
        {
            // not found so far; run through the set of elements available at this level
            foreach( $productElements as $productElement )
            {
                // if the element being tested is an array
                if( is_array( $productElement ) && !empty( $productElement ) )
                {
                    // try to get the missing element from that array
                    if( $missingElementValue = $this->getProductElement( $productElement, $missingElement ) )
                    {
                        break;
                    }
                }
            }
        }

        // return the result
        return $missingElementValue;
    }

    /**
     * Get the data tabs containing the correct language variables
     *
     * @param null|int $productId
     * @param string   $languageCode
     * @param bool     $resetCache
     *
     * @return array data tabs array
     */
    public function getDataTabsForLanguageWithEcommerceLinks( $productId = null, $languageCode = 'en', $resetCache = false )
    {
        // define the cache key
        $cacheKey = __FUNCTION__ . ':20151216:' . ( is_null( $productId ) ? 0 : $productId ) . ':' . $languageCode;

        // get the data tabs from the LRU
        if( !( $tabs = $this->cacheGet( $cacheKey ) ) || $resetCache )
        {
            $this->debugMessage( ( "Miss LRU Cache $cacheKey" ) );
            // get the maximum set of data tabs for the category for the language
            $tabs = $this->getDataTabsForCategoryForLanguage( $productId, $languageCode, true, $resetCache );

            // if we were passed a product_id
            if( $productId )
            {
                // get the set of checked product attributes
                $this->getCheckedProductAttributes( $productId, $tabs, $resetCache );
            }

            // return the final values
            $tabs = array_values( $tabs );

            // set the item in the LRU
            $this->debugMessage( ( "Set LRU Cache $cacheKey" ) );
            $this->cacheSet( $cacheKey, $tabs );
        }
        else
        {
            $this->debugMessage( ( "Hit LRU Cache $cacheKey" ) );
        }

        return $tabs;
    }

    /**
     * Get the data tabs containing the correct language variables
     *
     * @internal param string $languageCode
     *
     * @return array data tabs array
     */
    public function getDataTabsAttributesOrdered()
    {
        // create the question SQL
        $tabSQL = "SELECT product_attribute_type_name
			FROM product_data_tab
			JOIN product_attribute_type ON product_data_tab.product_attribute_type_id=product_attribute_type.product_attribute_type_id
			GROUP BY product_attribute_type_id
			ORDER BY precedence DESC, product_attribute_type_id ASC ";

        // execute the query
        return $this->_db->createCommand( $tabSQL )
                         ->queryAll();
    }

    /**
     * Get the set of users recommended for a product / user_id pair
     *
     * @param int    $productId
     * @param string $limit
     * @param bool   $ignoreCache
     * @param int    $userId
     *
     * @return array
     */
    public function getProductRecommendedBy( $productId, $limit = "0,10", $ignoreCache, $userId = null )
    {
        $this->deltaTimer( 'START getProductRecommendedBy' );

        // if we haven't been passed a user
        if( is_null( $userId ) )
        {
            // Get the logged in username
            if( is_null( $userName = YiiItcher::app()->user->getId() ) )
            {
                // if it's guest, return null array
                return array();
            }

            /**
             * Get the user_id for that username
             * @var $user User
             */
            $user = User::model()
                        ->findByAttributes( array('username' => $userName) );
            $userId = $user->user_id;
        }

        // check to see if we already have a list for this userId and productId in the cache
        $getProductRecommendedByCacheKey = __CLASS__ . '::getProductRecommendedBy-' . $userId . '-' . $productId;
        if( !( $recommendedBy = $this->_cache->get( $getProductRecommendedByCacheKey ) ) || $ignoreCache )
        {
            // get the set of similar users by category id for this user
            // check to see if we already have a set of similar users by category id for this user in memory
            if( !is_null( $this->_productRecommendedUsers ) )
            {
                $similarUsersByCategory = $this->_productRecommendedUsers;
            }
            else
            {
                // if we haven't already got data for the user
                $similarUsersByCategoryCacheKey = __CLASS__ . '::similarUsersByCategory-' . $userId;
                if( !( $similarUsersByCategory = $this->_cache->get( $similarUsersByCategoryCacheKey ) ) || $ignoreCache )
                {
                    /**
                     * get the set of similar users by category id for this user
                     * @var $similarUsersByCategoryId SimilarUserByCategoryId
                     */
                    $similarUsersByCategoryId = SimilarUserByCategoryId::model()
                                                                       ->findByPk( $userId );
                    if( $similarUsersByCategoryId )
                    {
                        // decode the list
                        $similarUsersByCategory = json_decode( $similarUsersByCategoryId->similar_user_by_category_id, true );

                        // and cache the results
                        $this->_cache->set( $similarUsersByCategoryCacheKey, $similarUsersByCategory, self::$_SIMILAR_USERS_CACHE_TIME );
                    }
                }

            }

            // set a blank array
            $recommendedBy = array();
            // if we have some similar users by category
            if( $similarUsersByCategory )
            {
                // get the root category id for the product
                $rootCategoryId = Product::model()
                                         ->getRootCategoryId( $productId );

                // if we have one
                if( $rootCategoryId != -1 )
                {
                    // and we have similar users for the category
                    if( Utilities::arrayCheck( $similarUsersByCategory, $rootCategoryId ) )
                    {
                        // get the as a CSV
                        $similarUsersCheckCSV = Utilities::generateCsv( array_keys( $similarUsersByCategory[$rootCategoryId] ), '' );

                        // look up the similar users who recommended this product
                        $similarUserSQL = "SELECT user.user_id, user.name, user.username, user.user_profile_picture, user.source, user_rating.rating
                            FROM user_rating
                            JOIN user ON user_rating.user_id = user.user_id
                            WHERE product_id = $productId
                            AND user.user_id IN ($similarUsersCheckCSV)
                            AND user.user_id != $userId";
                        $similarUsersForProduct = YiiItcher::app()->db->createCommand( $similarUserSQL )
                                                                      ->queryAll( true );

                        // run through the list of matches
                        foreach( $similarUsersForProduct as $similarUsersForProductCheck )
                        {
                            // generate the elements to return
                            $userName = ( strlen( $similarUsersForProductCheck['name'] ) ? $similarUsersForProductCheck['name'] : $similarUsersForProductCheck['username'] );

                            // if we don't have a picture for this user, and they're a non-itcher user
                            $userProfilePicture = $similarUsersForProductCheck['user_profile_picture'];
                            if( $userProfilePicture == null && $similarUsersForProductCheck['source'] > 0 && strlen( trim( $userName ) ) )
                            {
                                // set the picture to be loaded asynchronously
                                $userProfilePicture = YiiItcher::app()->params['imagesRootURI'] . 'image/user/id/' . $similarUsersForProductCheck['user_id'] . '/width/40/title/' . $similarUsersForProductCheck['user_id'] . '.jpg';
                            }
                            else
                            {
                                if( $userProfilePicture != null )
                                {
                                    $userProfilePicture = YiiItcher::app()->params['imagesRootURI'] . 'image/user/id/' . $similarUsersForProductCheck['user_id'] . '/width/40/title/' . basename( $userProfilePicture );
                                }
                            }

                            // use it as is
                            $similarity = round( $similarUsersByCategory[$rootCategoryId][$similarUsersForProductCheck['user_id']] );

                            // add the data to the final recommendations array
                            $recommendedBy[] = array(
                                'user_id'              => $similarUsersForProductCheck['user_id'],
                                'user_name'            => $userName,
                                'user_profile_picture' => $userProfilePicture,
                                'similarity'           => $similarity,
                                'rating'               => $similarUsersForProductCheck['rating']
                            );

                            // add the similarity to the sort array
                            $similarities[] = $similarity;
                        }

                        // if we have a list
                        if( count( $recommendedBy ) )
                        {
                            // sort final recommendations array by similarity
                            array_multisort( $similarities, SORT_DESC, $recommendedBy );
                        }
                    }
                }
            }

            // and cache the results
            $this->_cache->set( $getProductRecommendedByCacheKey, $recommendedBy, ( count( $recommendedBy ) ? self::$_PRODUCT_CACHE_TIME : 60 ) );

        }
        $this->deltaTimer( 'END getProductRecommendedBy' );

        // return the slice requested
        $offsetLimit = explode( ',', $limit );

        return array_slice( $recommendedBy, $offsetLimit[0], $offsetLimit[1] );
    }

    /**
     * Check that the product name exists in the database
     *
     * @param string $productName
     * @param bool   $ignoreCache
     * @param bool   $useIgnoreList
     *
     * @return boolean
     */
    public function validProductName( $productName, $ignoreCache = false, $useIgnoreList = false )
    {
        if( $useIgnoreList && in_array( strtolower( $productName ), LinkItcherMagContentCommand::$ignoreList ) )
        {
            return false;
        }

        // check to see if we already have a product for this currentNoun and categoryName in the cache
        $cacheKey = __CLASS__ . ':validProductName:' . $productName;
        if( !( $product = $this->cacheGet( $cacheKey ) ) || $ignoreCache )
        {
            $searchPhrase = addslashes( strtolower( $productName ) );
            $productNameCheck = "(product_name = '$searchPhrase' OR product_name LIKE '$searchPhrase %' OR product_name LIKE '$searchPhrase: %')";
            // if the search phrase has an "AND" in it we need an alternative "&"
            if( strpos( $searchPhrase, ' and ' ) !== false )
            {
                $productNameCheck .= " OR product_name = '" . str_replace( ' and ', ' & ', $searchPhrase ) . "'";
            }

            // get the top product which matches the product name
            $productSQL = "SELECT p.product_id
                FROM product p
                JOIN category_product cp on p.product_id=cp.product_id
                WHERE display = 1
                AND $productNameCheck
                -- AND is_subproduct = 0
                LIMIT 1
                ";
            $product = YiiItcher::app()->db->createCommand( $productSQL )
                                           ->queryRow();
            echo( $productName );
            if( !$product )
            {
                echo( " - not a product - check for actor" );
                // if we didn't find them, try to find an actor
                // check to see if it's a username (Actor) from TMDB
                $userSQL = "SELECT user_id, SUBSTRING_INDEX(username, '::', -1) AS tmdb_id, name, user_profile_picture, date_created, bio, location
                    FROM user u
                    WHERE u.username LIKE '" . $searchPhrase . "::tmdb::%'
                    LIMIT 1";
                $user = YiiItcher::app()->db->createCommand( $userSQL )
                                            ->queryRow( true );

                // if we didn't find an actor
                if( !$user )
                {
                    echo( " - not an actor - check for author" );
                    // try to find an author
                    $authorSQL = "SELECT 1 FROM product_attribute NATURAL JOIN product_attribute_type
                        WHERE product_attribute_type_name = 'author'
                        AND product_attribute_value = '" . $searchPhrase . "'
                        LIMIT 1";
                    $author = YiiItcher::app()->db->createCommand( $authorSQL )
                                                  ->queryRow( true );

                    if( !$author )
                    {
                        echo( " - not an author - not valid\n" );
                        $product = 'not found';
                    }
                    else
                    {
                        echo( " - valid author\n" );
                        $product = 1;
                    }
                }
                else
                {
                    echo( " - valid actor\n" );
                    $product = 1;
                }
            }
            else
            {
                echo( " - valid product\n" );
                $product = 1;
            }

            // and cache the results
            $this->cacheSet( $cacheKey, $product, self::$_PRODUCT_CACHE_TIME );
        }
        else
        {
            if( $product == 'not found' )
            {
                echo( "Product $productName not valid from cache\n" );
            }
            else
            {
                echo( "Product $productName valid from cache\n" );
            }
        }
        if( $product == 'not found' )
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * Get the top product by name by category
     *
     * @param array  $properNouns             - array of proper nouns
     * @param int    $currentNoun             - offset of current proper noun
     * @param string $categoryName            - name of category
     * @param int    $imageSize               - size of image required
     * @param array  $potentialAuthorOrArtist - potential name of the author or artist
     * @param bool   $editorialLink           - editorial link flag
     * @param bool   $ignoreCache             - ignore cache flag
     *
     * @return mixed
     */
    public function getProductByNameByCategory( $properNouns, $currentNoun, $categoryName, $imageSize, $potentialAuthorOrArtist, $editorialLink = false, $ignoreCache = false )
    {
        // check to see if we already have a product for this currentNoun and categoryName in the cache
        $currentProperNoun = $properNouns[$currentNoun];
        $nextProperNoun = ( $currentNoun != count( $properNouns ) - 1 ? $properNouns[$currentNoun + 1] : 'last' );
        $cacheKey = __CLASS__ . '::getProductByNameByCategory_v2::' . $currentProperNoun . '-' . $nextProperNoun . '-' . $categoryName . '-' . ( $editorialLink ? 'yes' : 'no' ) . '-' . $imageSize;
        if( !( $product = $this->cacheGet( $cacheKey ) ) || $ignoreCache )
        {
            // get the categoryID for the categoryName passed
            $categoryId = Category::model()
                                  ->getIdFromName( $categoryName == 'games' ? 'video games' : $categoryName );

            // set a null check attribute
            $checkAttributeTypeId = null;

            // set product to not found
            $product = 'not found';

            // set that we need to get extended product information
            $extendedProduct = true;

            // set up search phrase
            $searchPhrase = addslashes( $properNouns[$currentNoun] );

            // if we have a books lookup
            if( $categoryName == 'books' )
            {
                // if the search phrase has an "AND" in it we need an alternative "&"
                if( strpos( $searchPhrase, ' and ' ) !== false )
                {
                    $searchPhrase .= "','" . str_replace( ' and ', ' & ', $searchPhrase );
                }

                // get the author from the attributes
                $checkAttributeTypeId = ProductAttributeType::model()
                                                            ->getIdByName( 'author' );

                // get the top product which matches the product name and category and author
                $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, p.product_id, product_name, product_description, is_subproduct, root_category_id, product_average_rating, product_num_ratings, product_num_reviews
                        FROM product p
                        JOIN category_product cp on p.product_id=cp.product_id
                        JOIN category c on cp.category_id=c.category_id
                        JOIN product_attribute pa on pa.product_id = p.product_id
                        WHERE display = 1 AND root_category_id = :category_id
                        AND product_name IN ('$searchPhrase')
                        AND is_subproduct = 0
                        AND pa.product_attribute_type_id = :check_attribute_id
                        AND pa.product_attribute_value = :check_attribute_value
                        GROUP BY product_name_md5, root_category_id
                        ORDER BY product_weighted_rating DESC
                        LIMIT 1
                        ";

                if( $ignoreCache )
                {
                    $this->debugMessage( $productSQL );
                }

                $command = YiiItcher::app()->db->createCommand( $productSQL );

                // if we're not at the end of the set
                if( $currentNoun != count( $properNouns ) - 1 )
                {
                    $this->debugMessage( "    Lookup " . $properNouns[$currentNoun] . " with author " . $properNouns[$currentNoun + 1] );
                    // try to get the product using the proper noun after as the author
                    $product = $command->queryRow( true, array(
                            ':category_id'           => $categoryId,
                            ':check_attribute_id'    => $checkAttributeTypeId,
                            ':check_attribute_value' => $properNouns[$currentNoun + 1],
                        ) );
                    if( $product )
                    {
                        $this->debugMessage( "     - found" );
                    }
                    else
                    {
                        $product = 'not found';
                        $this->debugMessage( "     - not found" );
                    }
                }

                // if we didn't get a match and this isn't the first in the set
                if( $product == 'not found' && $currentNoun != 0 )
                {
                    $this->debugMessage( "    Lookup " . $properNouns[$currentNoun] . " with author " . $properNouns[$currentNoun - 1] );
                    // try to get the product using the proper noun before it as the author
                    $product = $command->queryRow( true, array(
                            ':category_id'           => $categoryId,
                            ':check_attribute_id'    => $checkAttributeTypeId,
                            ':check_attribute_value' => $properNouns[$currentNoun - 1],
                        ) );
                    if( $product )
                    {
                        $this->debugMessage( "     - found" );
                    }
                    else
                    {
                        $product = 'not found';
                        $this->debugMessage( "     - not found" );
                    }
                }

                // if we didn't get a match and we have a potential author match
                if( $product == 'not found' && count( $potentialAuthorOrArtist ) )
                {
                    foreach( $potentialAuthorOrArtist as $potentialAuthor )
                    {
                        $this->debugMessage( "    Lookup " . $properNouns[$currentNoun] . " with author " . $potentialAuthor );
                        // try to get the product using the potential author
                        $product = $command->queryRow( true, array(
                                ':category_id'           => $categoryId,
                                ':check_attribute_id'    => $checkAttributeTypeId,
                                ':check_attribute_value' => $potentialAuthor,
                            ) );
                        if( $product )
                        {
                            $this->debugMessage( "     - found" );
                            break;
                        }
                    }

                    if( !$product )
                    {
                        $product = 'not found';
                        $this->debugMessage( "     - not found" );
                    }

                }

                /*
                // if we still didn't get a match
                if( $product == 'not found' && strpos($searchPhrase, ' ') !== false)
                {
                    // get a match set containing disambiguated author names
                    $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, p.product_id, product_name, product_description, is_subproduct, root_category_id, product_average_rating, product_num_ratings, product_num_reviews,
                        REPLACE(REPLACE(pa.product_attribute_value, '.', ' '), '  ', ' ') AS author
                        FROM product p
                        JOIN category_product cp on p.product_id=cp.product_id
                        JOIN category c on cp.category_id=c.category_id
                        JOIN product_attribute pa on pa.product_id = p.product_id
                        WHERE display = 1 AND root_category_id = :category_id
                        AND product_name IN ('$searchPhrase')
                        AND is_subproduct = 0
                        AND pa.product_attribute_type_id = :check_attribute_id
                        GROUP BY product_name_md5, root_category_id, author
                        ORDER BY product_weighted_rating DESC
                        LIMIT 10
                        ";

                    if( $ignoreCache )
                    {
                        $this->debugMessage( $productSQL );
                    }

                    $this->debugMessage( "    Lookup " . $searchPhrase . " for unique author " );

                    $products = YiiItcher::app()->db->createCommand( $productSQL )->queryAll( true, array(
                        ':category_id'        => $categoryId,
                        ':check_attribute_id' => $checkAttributeTypeId,
                    ) );;

                    // if there's only a single author
                    if( count( $products ) == 1 )
                    {
                        $this->debugMessage( "     - found" );
                        $product = $products[0];
                    }
                    elseif( count( $products ) > 1 )
                    {
                        $authors = '';
                        foreach( $products as $product )
                        {
                            if( strlen( $authors ) )
                            {
                                $authors .= ', ';
                            }
                            $authors .= $product['author'];
                        }
                        $product = 'not found';
                        $this->debugMessage( "     - multiple authors ($authors)" );
                    }
                    else
                    {
                        $product = 'not found';
                        $this->debugMessage( "     - not found" );
                    }
                }
                */

            }

            // if we don't yet have a product match
            elseif( $categoryName == 'movies' )
            {
                // count how many words in the phrase
                $words = explode( ' ', $properNouns[$currentNoun] );

                // if greater than 3 set it to be an editorial link as it's rare
                if( count( $words ) > 3 )
                {
                    $editorialLink = true;
                }

                // if the search phrase has an "AND" in it we need an alternative "&"
                $lookupPhrase = $searchPhrase;
                if( strpos( $lookupPhrase, ' and ' ) !== false )
                {
                    $lookupPhrase .= "','" . str_replace( ' and ', ' & ', $lookupPhrase );
                }

                // get the top product which matches the product name and category or actor
                // if it's an editorial link we don't care how many ratings there are otherwise we do
                $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, p.product_id, product_name, product_description, is_subproduct, root_category_id, product_average_rating, product_num_ratings, product_num_reviews
                FROM product p
                JOIN category_product cp on p.product_id=cp.product_id
                JOIN category c on cp.category_id=c.category_id
                JOIN product_attribute pa on pa.product_id = p.product_id
                WHERE display = 1 AND root_category_id = :category_id
                AND product_name IN ('$lookupPhrase')
                AND is_subproduct = 0
                AND release_date != 0
                AND product_num_ratings >= " . ( $editorialLink ? 2 : 10 ) . "
                GROUP BY product_name_md5, root_category_id, release_date
                ORDER BY product_weighted_rating DESC
                LIMIT 1
                ";

                if( $ignoreCache )
                {
                    $this->debugMessage( $productSQL );
                }

                $command = YiiItcher::app()->db->createCommand( $productSQL );

                $this->debugMessage( "    Lookup '$searchPhrase' in movies " );
                // try to get the product using the proper noun after as the author
                $product = $command->queryRow( true, array(
                        ':category_id' => $categoryId,
                    ) );
                if( $product )
                {
                    // discard lookups for single keywords with low ratings
                    if( strpos( $properNouns[$currentNoun], ' ' ) === false && $product['product_num_ratings'] < 40 )
                    {
                        $this->debugMessage( " - discarded (ratings " . $product['product_num_ratings'] . ")" );
                        $product = 'not found';
                    }
                    else
                    {
                        $this->debugMessage( "     - found" );
                    }
                }
                // if we didn't find it as a movie and it's not an editorial link
                elseif( !$editorialLink )
                {
                    // if we can find it as an actor with more than a single keyword
                    if( strpos( $properNouns[$currentNoun], ' ' ) !== false && !is_null( $product = $this->getActorInformation( $properNouns[$currentNoun], $ignoreCache ) ) )
                    {
                        // replace iLink etc
                        $product['product_description'] = preg_replace( "/\[iLink name='([^']+)' id='\d+'\]/", "$1", str_replace( '<br>', ' ', str_replace( '[iHl]', '&nbsp', str_replace( '[/iHl]', ':&nbsp;', $product['product_description'] ) ) ) );

                        // set that we don't need to get extended product information
                        $extendedProduct = false;
                        $this->debugMessage( "     - found as actor" );
                    }
                    else
                    {
                        $product = 'not found';
                        $this->debugMessage( "     - not found" );
                    }
                }
                else
                {
                    $product = 'not found';
                    $this->debugMessage( "     - not found" );
                }
            }
            // if we don't yet have a product match
            elseif( $categoryName == 'games' )
            {
                // if it's more than one word, check for "the" in front as well
                if( strpos( $searchPhrase, ' ' ) !== false )
                {
                    $searchPhrase .= "','the $searchPhrase";
                }

                $this->debugMessage( "    Lookup '$searchPhrase' in $categoryName" );

                // get the top product which matches the product name and category
                $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, IF (LENGTH(product_name) = LENGTH('battlefield'), 0, 1) AS relevance,
                p.product_id, product_name, product_description, is_subproduct, root_category_id,
                product_average_rating, product_num_ratings, product_num_reviews,
                MAX(pri.image_url)
                FROM product p
                JOIN category_product cp on p.product_id=cp.product_id
                JOIN category c on cp.category_id=c.category_id
                JOIN product_attribute pa on pa.product_id = p.product_id
                JOIN product_image pri ON p.`product_id` = pri.`product_id`
                WHERE display = 1 AND root_category_id = $categoryId
                AND product_name IN ('$searchPhrase')
                AND is_subproduct = 0
                -- AND product_weighted_rating != 0
                GROUP BY product_name_md5, root_category_id
                ORDER BY relevance ASC, release_date DESC, product_weighted_rating DESC
                LIMIT 1
                ";

                if( $ignoreCache )
                {
                    $this->debugMessage( $productSQL . "\n" );
                }

                // get the product
                $product = YiiItcher::app()->db->createCommand( $productSQL )
                                               ->queryRow( true );
                if( $product )
                {
                    $this->debugMessage( "     - found" );
                }
                else
                {
                    $this->debugMessage( "     - not found" );
                    $product = 'not found';
                }
            }
            // if we don't yet have a product match
            else
            {
                // if the search phrase has an "AND" in it we need an alternative "&"
                if( stripos( $searchPhrase, ' and ' ) !== false )
                {
                    $searchPhrase .= "','" . str_ireplace( ' and ', ' & ', $searchPhrase );
                }

                // if the search phrase starts with "The " we need to try with it at the end
                if( stripos( $properNouns[$currentNoun], 'The ' ) === 0 )
                {
                    $searchPhrase .= "','" . substr( $searchPhrase, strpos( $searchPhrase, ' ' ) + 1 ) . ", the";
                }
                // else
                else
                {
                    // add "the" to the front
                    $searchPhrase .= "','the " . $searchPhrase . "','" . $searchPhrase . ", the";
                }

                $this->debugMessage( "Lookup artist '$searchPhrase' in music" );
                // get the top product which matches the product name and category
                $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, p.product_id, product_name, product_description, is_subproduct, root_category_id, product_weighted_rating, product_average_rating, product_num_ratings, product_num_reviews
                FROM product p
                JOIN category_product cp on p.product_id=cp.product_id
                JOIN category c on cp.category_id=c.category_id
                JOIN product_attribute pa on pa.product_id = p.product_id
                WHERE display = 1 AND root_category_id = :category_id
                AND product_name IN ('$searchPhrase')
                AND is_subproduct = 0
                AND product_weighted_rating != 0
                GROUP BY p.product_id, root_category_id
                ORDER BY product_weighted_rating DESC
                LIMIT 1
                ";

                if( $ignoreCache )
                {
                    $this->debugMessage( $productSQL );
                }

                // get the product
                $command = YiiItcher::app()->db->createCommand( $productSQL );
                $product = $command->queryRow( true, array(
                        ':category_id' => $categoryId,
                    ) );
                if( !$product )
                {
                    $product = 'not found';
                }

                // if we're not at the end of the set
                if( $product == 'not found' && $currentNoun != count( $properNouns ) - 1 )
                {
                    $this->debugMessage( "    Lookup " . $properNouns[$currentNoun] . " with artist " . $properNouns[$currentNoun + 1] );

                    // get the top product which matches the product name and category
                    $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, p2.product_id, p2.product_name, p2.product_description, p2.is_subproduct, root_category_id, p2.product_weighted_rating, p2.product_average_rating, p2.product_num_ratings, p2.product_num_reviews
                        FROM product p
                        JOIN category_product cp ON p.product_id=cp.product_id
                        JOIN category c ON cp.category_id=c.category_id
                        JOIN product_subproduct ps ON p.`product_id` = ps.product_id
                        JOIN product p2 ON p2.product_id = ps.subproduct_id
                        WHERE p.display = 1
                        AND root_category_id = :category_id
                        AND p.product_name = '" . addslashes( $properNouns[$currentNoun + 1] ) . "'
                        AND p2.product_name = '" . addslashes( $properNouns[$currentNoun] ) . "'
                        AND p.is_subproduct = 0
                        AND p2.is_subproduct = 1
                        GROUP BY p.product_id, root_category_id
                        ORDER BY p.product_weighted_rating DESC
                        LIMIT 1
                        ";
                    // get the product
                    $command = YiiItcher::app()->db->createCommand( $productSQL );
                    $product = $command->queryRow( true, array(
                            ':category_id' => $categoryId,
                        ) );

                    if( !$product )
                    {
                        $product = 'not found';
                    }
                }

                // if we didn't get a match and this isn't the first in the set
                if( $product == 'not found' && $currentNoun != 0 )
                {
                    $this->debugMessage( "    Lookup " . $properNouns[$currentNoun] . " with artist " . $properNouns[$currentNoun - 1] );
                    // get the top product which matches the product name and category
                    $productSQL = "SELECT '" . addslashes( $properNouns[$currentNoun] ) . "' AS search_name, p2.product_id, p2.product_name, p2.product_description, p2.is_subproduct, root_category_id, p2.product_weighted_rating, p2.product_average_rating, p2.product_num_ratings, p2.product_num_reviews
                        FROM product p
                        JOIN category_product cp ON p.product_id=cp.product_id
                        JOIN category c ON cp.category_id=c.category_id
                        JOIN product_subproduct ps ON p.`product_id` = ps.product_id
                        JOIN product p2 ON p2.product_id = ps.subproduct_id
                        WHERE p.display = 1
                        AND root_category_id = :category_id
                        AND p.product_name = '" . addslashes( $properNouns[$currentNoun - 1] ) . "'
                        AND p2.product_name = '" . addslashes( $properNouns[$currentNoun] ) . "'
                        AND p.is_subproduct = 0
                        AND p2.is_subproduct = 1
                        GROUP BY p.product_id, root_category_id
                        ORDER BY p.product_weighted_rating DESC
                        LIMIT 1
                        ";
                    // get the product
                    $command = YiiItcher::app()->db->createCommand( $productSQL );
                    $product = $command->queryRow( true, array(
                            ':category_id' => $categoryId,
                        ) );

                    if( !$product )
                    {
                        $product = 'not found';
                    }
                }

                // if we got a product
                if( $product != 'not found' )
                {
                    // discard lookups for single keywords with low ratings which are artists
                    if( strpos( $properNouns[$currentNoun], ' ' ) === false && $product['is_subproduct'] == 0 && $product['product_weighted_rating'] < 1 )
                    {
                        $this->debugMessage( " - discarded (rating " . $product['product_weighted_rating'] . ")\n" );
                        $product = 'not found';
                    }
                    $this->debugMessage( "     - found" );
                }
                else
                {
                    $this->debugMessage( "     - not found" );
                }
            }

            // if we got a product
            if( $product != 'not found' && $extendedProduct )
            {
                // finally queue the product for update on Amazon so we update reviews, etc
                $amazonDataProvider = new AmazonDataProvider();
                $amazonDataProvider->init( 'GB' );
                $amazonDataProvider->searchProductsQueued( array(
                    'category_id'     => $categoryId,
                    'product_id'      => $product['product_id'],
                    'product_name'    => $properNouns[$currentNoun],
                    "scrapeImages"    => true,
                    "onlyUpdateEmpty" => true
                ), true );

            }

            // and cache the results
            $this->cacheSet( $cacheKey, $product, self::$_PRODUCT_CACHE_TIME );

        }
        else
        {
            if( $product == 'not found' )
            {
                $this->debugMessage( "    " . $properNouns[$currentNoun] . " not found from cache" );
                $product = null;
            }
            else
            {
                $this->debugMessage( "    " . $properNouns[$currentNoun] . " found from cache" );
            }
        }

        return $product;
    }

    /**
     * Get product information including primary square image, description, name, product details url and related products
     *
     * @param int  $productId
     *
     * @param bool $resetCache
     *
     * @return array
     */
    public function getProductTooltipInformation( $productId, $resetCache = false )
    {
        $product = array('product_id' => $productId);

        // get the needed product elements through the cache
        $this->getProductElementsThroughCache( $product, array(
                'product_short_description',
                'square_image_url',
                'categories',
                'product_average_rating',
                'product_num_ratings',
                'product_num_reviews'
            ), $resetCache );

        /**
         * @var string $encodedName the name and id of the product for use in the product link url
         */
        $encodedName = preg_replace( "/\.|\,|\;|&|!|'|\||\s|\[|\]|\{|\}|\(|\)|:|\+|\/|\\\/", '-', $product['product_name'] );

        // Normalize multiple dashes
        $encodedName = preg_replace( "/-{2,}/", '-', $encodedName );

        // Edge case - trailing dashes
        $encodedName = rtrim( $encodedName, '-' );
        $encodedName = urlencode( $encodedName . '-' . $productId );

        // Get the root category name
        $rootCategories = $this->getCategoriesForCountry( 'GB' );
        foreach( $rootCategories as $rootCategory )
        {
            if( $rootCategory['category_id'] == $product['root_category_id'] )
            {
                $category = $rootCategory['category_name'];
            }
        }

        // Construct the link to the product details page in itcher
        if( isset( $category ) )
        {
            $product['product_link'] = YiiItcher::app()->params['baseURI'] . strtolower( str_replace( ' ', '-', $category ) ) . '/' . $encodedName . '/';
        }

        // get the related Items
        $relatedItems = $this->getRelatedItems( $product['product_id'], $resetCache );
        if( !empty( $relatedItems ) )
        {
            $relatedItemCount = 0;
            foreach( $relatedItems as $relatedItem )
            {
                if( $relatedItemCount < 5 )
                {
                    $product['related_items'][$relatedItemCount]['product_name'] = $relatedItem['product_name'];
                    $product['related_items'][$relatedItemCount]['product_id'] = $relatedItem['product_id'];
                    $product['related_items'][$relatedItemCount]['product_image'] = str_replace( "/180/", "/90/", $relatedItem['square_image_url'] );
                    $relatedItemCount += 1;
                }
            }
            // add to the product attributes

        }

        return $product;
    }

    /**
     * Get the top weighted product by id by category
     *
     * @param int    $productID
     * @param string $productName
     * @param string $categoryName
     *
     * @return mixed
     */
    public function getProductByIdByCategory( $productID, $productName, $categoryName )
    {
        // check to see if we already have a product for this productId and categoryName in the cache
        $cacheKey = __CLASS__ . '::getProductByIdByCategory::201410221655::' . $productID . '-' . $productName . '-' . $categoryName;
        if( !( $product = $this->cacheGet( $cacheKey ) ) )
        {
            // get the categoryID for the categoryName passed
            $categoryId = Category::model()
                                  ->getIdFromName( $categoryName );

            echo( "Lookup $productID in $categoryName" );
            // get the top product which matches the product name and category and author
            $productSQL = "SELECT :product_name AS search_name, p.product_id, product_name, product_description, is_subproduct, root_category_id, product_average_rating, product_num_ratings, product_num_reviews
                FROM product p
                JOIN category_product cp ON p.product_id=cp.product_id
                JOIN category c ON cp.category_id=c.category_id
                JOIN product_attribute pa ON pa.product_id = p.product_id
                WHERE display = 1 AND root_category_id = :category_id
                AND p.product_id = :product_id
                GROUP BY root_category_id
                ORDER BY product_weighted_rating DESC
                LIMIT 1
                ";

            $product = YiiItcher::app()->db->createCommand( $productSQL )
                                           ->queryRow( true, array(
                                                   ':category_id'  => $categoryId,
                                                   ':product_id'   => $productID,
                                                   ':product_name' => $productName
                                               ) );
            if( $product )
            {
                echo( " - found" );
            }
            else
            {
                $product = 'not found';
                echo( " - not found\n" );
            }

        }

        // and cache the results
        $this->cacheSet( $cacheKey, $product, self::$_PRODUCT_CACHE_TIME );

        return $product;
    }

    public function getProductNumbersByCategory()
    {
        return 1;
    }

    /**
     * Add / update / delete the search index for a product on CloudFront
     *
     * @param array $productIds
     * @param bool  $includeVersion
     * @param bool  $forceDelete
     * @param bool  $debug
     * @param bool  $ignoreDelete
     *
     * @return mixed|null
     */
    public function updateProductSearchIndex( $productIds, $includeVersion = false, $forceDelete = false, $debug = false, $ignoreDelete = false )
    {
        $this->debugMessage( 'updateProductSearchIndex: processing ' . count( $productIds ) . " items" );
        // get the current time for updating the
        $timeNow = floor( microtime( true ) );

        // get an array of root categories for the country
        $checkCategories = array();
        $rootCategories = $this->getCategoriesForCountry( 'GB' );   // GB only for now
        foreach( $rootCategories as $rootCategory )
        {
            $checkCategories[$rootCategory['category_id']] = $rootCategory['category_id'];
        }

        // if a single value has been passed
        if( !empty( $productIds ) && !is_array( $productIds ) )
        {
            // create an array containing it
            $productIds = array(
                $productIds
            );
        }

        // if we have some productIds
        if( !empty( $productIds ) )
        {
            // create the array
            $cloudSearchSDF = array();

            // if we're not deleting everything
            if( !$forceDelete )
            {
                // create SQL to check product validity
                $validationSQL = "SELECT p.product_id FROM product p JOIN category_product cp ON p.product_id=cp.product_id JOIN category c ON cp.category_id = c.category_id
                              WHERE p.product_id IN (" . implode( ',', $productIds ) . ") AND p.display=1 AND p.archived=0
                              AND c.root_category_id IN (" . implode( ',', $checkCategories ) . ") GROUP BY p.product_id;";

                // query database for valid product ids
                $validProductIds = $this->_db->createCommand( $validationSQL )
                                             ->queryColumn();

                // set all product ids that aren't valid as invalid
                $invalidProductIds = array_diff( $productIds, $validProductIds );

                // run through the set of valid productIds
                foreach( $validProductIds as $productId )
                {
                    // get the underlying product data
                    $productDetails = $this->getProductsByProductId( $productId );

                    // if the product is in an invalid root category despite passing the check earlier
                    $rootCategory = $productDetails['root_category_id'];
                    if( !isset( $checkCategories[$rootCategory] ) )
                    {
                        // it's probably in a valid root category as well so look that up in the database
                        $rootCategorySQL = "SELECT root_category_id FROM category_product NATURAL JOIN category WHERE product_id=" . $productId . " AND root_category_id IN (" . implode( ',', $checkCategories ) . ") GROUP BY root_category_id LIMIT 1;";

                        $result = $this->_db->createCommand( $rootCategorySQL )
                                            ->queryScalar();
                        if( !empty( $result ) )
                        {
                            // if we found a valid root category update the root category of the product before uploading it to amazon cloudsearch
                            $productDetails['root_category_id'] = $result;
                        }
                        else
                        {
                            // if we didn't somethings gone wrong so add it to the list of invalid products and skip it.
                            $invalidProductIds[] = $productId;
                            continue;
                        }
                    }

                    // escape '-'s
                    $productDetails['product_name'] = str_replace( 'XZX', ' - ', str_replace( '-', '', str_replace( ' - ', 'XZX', $productDetails['product_name'] ) ) );
                    $productDetails['product_sort_name'] = str_replace( 'XZX', ' - ', str_replace( '-', '', str_replace( ' - ', 'XZX', $productDetails['product_sort_name'] ) ) );

                    // get parent categories
                    $productDetails['categories'] = $this->fillCategoriesArray( $productDetails['categories'] );

                    if( empty( $productDetails['product_description'] ) && !empty( $productDetails['product_attributes']['wikipedia'][0]['Introduction'] ) )
                    {
                        // if the product has no description but we have a wikipedia description of it use the introduction as the description
                        $productDetails['product_description'] = $productDetails['product_attributes']['wikipedia'][0]['Introduction'];

                        // truncate overlong product descriptions
                        $productDetails['product_short_description'] = mb_substr( preg_replace( "/\[iLink name='([^']+)' id='\d+'\]/", "$1", str_replace( '<br>', ' ', str_replace( '[iHl]', ' ', str_replace( '[/iHl]', ': ', trim( $productDetails['product_description'] ) ) ) ) ), 0, 512 );

                    }
                    // grab the necessary data from the details
                    $productFields = array();
                    foreach( $productDetails as $key => $value )
                    {
                        if( !empty( $value ) )
                            switch( $key )
                            {
                                case 'product_name':
                                case 'product_id':
                                case 'release_date':
                                case 'is_subproduct':
                                case 'product_num_ratings':
                                case 'product_num_reviews':
                                case 'root_category_id':
                                case 'product_short_description':
                                case 'product_description':
                                case 'product_average_rating':
                                    if( !empty( $value ) )
                                    {
                                        $productFields[$key] = $this->stripInvalidCodePoints( $value );
                                    }
                                    break;
                                case 'categories':
                                    $productFields[$key] = count( $value );
                                    $productFields['category_names'] = array();
                                    $productFields['category_ids'] = array();
                                    $productFields['categories_relevance'] = array();
                                    foreach( $value as $category )
                                    {
                                        $productFields['category_names'][] = $this->stripInvalidCodePoints( $category['category_name'] );
                                        $productFields['category_ids'][] = $this->stripInvalidCodePoints( $category['category_id'] );
                                        $productFields['categories_relevance'][] = $this->stripInvalidCodePoints( $category['relevance'] );
                                    }
                                    break;

                                case 'images':
                                    $productFields[$key] = count( $value );
                                    $productFields['image_names'] = array();
                                    $productFields['image_urls'] = array();
                                    $productFields['image_widths'] = array();
                                    $productFields['image_heights'] = array();
                                    $productFields['image_types'] = array();
                                    foreach( $value as $name => $image )
                                    {
                                        $productFields['image_names'][] = $this->stripInvalidCodePoints( $name );
                                        $productFields['image_urls'][] = $this->stripInvalidCodePoints( $image['image_url'] );
                                        $productFields['image_widths'][] = $this->stripInvalidCodePoints( $image['image_width'] );
                                        $productFields['image_heights'][] = $this->stripInvalidCodePoints( $image['image_height'] );
                                        $productFields['image_types'][] = $this->stripInvalidCodePoints( $image['image_type'] ?: "Undefined" );
                                    }
                                    break;

                                case 'publisher':
                                case 'platform':
                                case 'genre':
                                case 'developer':
                                case 'author':
                                case 'artist':
                                    while( is_array( $value ) )
                                    {
                                        $value = array_shift( $value );
                                    }
                                    $productFields[$key] = $this->stripInvalidCodePoints( $value );
                                    break;

                                case 'cast':
                                    $productFields['cast'] = array();
                                    $productFields['characters'] = array();
                                    foreach( $value as $cast )
                                    {
                                        if( isset( $cast['who'] ) && isset( $cast['what'] ) )
                                        {
                                            $productFields['cast'][] = $this->stripInvalidCodePoints( $cast['who'] );
                                            $productFields['characters'][] = $this->stripInvalidCodePoints( $cast['what'] );
                                        }
                                        elseif( isset( $cast['name'] ) && isset( $cast['character'] ) )
                                        {
                                            $productFields['cast'][] = $this->stripInvalidCodePoints( $cast['name'] );
                                            $productFields['characters'][] = $this->stripInvalidCodePoints( $cast['character'] );
                                        }
                                        else
                                        {
                                            while( is_array( $cast ) )
                                            {
                                                $cast = array_shift( $cast );
                                            }
                                            $productFields['cast'][] = $this->stripInvalidCodePoints( $cast );
                                            $productFields['characters'][] = $this->stripInvalidCodePoints( "unknown" );
                                        }
                                    }
                                    break;

                                case 'crew':
                                    $productFields['crew'] = array();
                                    foreach( $value as $crew )
                                    {
                                        if( isset( $crew['who'] ) )
                                        {
                                            $productFields['crew'][] = $this->stripInvalidCodePoints( $crew['who'] );
                                        }
                                        elseif( isset( $crew['name'] ) )
                                        {
                                            $productFields['crew'][] = $this->stripInvalidCodePoints( $crew['name'] );
                                        }
                                        else
                                        {
                                            while( is_array( $crew ) )
                                            {
                                                $crew = array_shift( $crew );
                                            }
                                            $productFields['crew'][] = $this->stripInvalidCodePoints( $crew );
                                        }
                                    }
                                    break;

                                case 'related_items':
                                    $productFields['related_items'] = array();
                                    foreach( $value as $related )
                                    {
                                        if( isset( $related['product_name'] ) )
                                        {
                                            $productFields['related_items'][] = $this->stripInvalidCodePoints( $related['product_name'] );
                                        }
                                    }
                                    break;

                            }
                    }

                    // find the keywords for the product
                    $keywords = $this->getKeywordsThroughCache( $productFields['product_id'] );
                    if( !empty( $keywords ) )
                    {
                        // if any keywords were found add them to the index
                        $productFields['keywords'] = $keywords;
                    }
                    $this->debugMessage( "Add product id: $productId to add batch", 2 );

                    // we have to deal with 2 versions of the API for now, one of which includes the version and one which doesn't
                    if( $includeVersion )
                    {
                        // create the SDF slug - setting the version to the current
                        // timestamp makes sure a current record gets updated
                        $cloudSearchSDF[] = array(
                            'type'    => 'add',
                            'id'      => $productId,
                            'version' => $timeNow,
                            'lang'    => 'en',
                            'fields'  => $productFields
                        );
                    }
                    else
                    {
                        // create the SDF slug - setting the version to the current
                        // timestamp makes sure a current record gets updated
                        $cloudSearchSDF[] = array(
                            'type'   => 'add',
                            'id'     => $productId,
                            'fields' => $productFields
                        );
                    }

                    // update the last_indexed column for the product
                    $product = Product::model()
                                      ->findByPk( $productId );
                    $product->setAttribute( 'last_indexed', new CDbExpression( 'UNIX_TIMESTAMP( NOW() )' ) );

                    // update the product with the last_indexed data
                    $product->save();
                }
            }
            else
            {
                // if we're forcing delete set all product ids as invalid
                $invalidProductIds = $productIds;
            }

            // if we're not skipping the deletion
            if( !$ignoreDelete )
            {
                // mark all invalid products for deletion
                foreach( $invalidProductIds as $productId )
                {
                    $this->debugMessage( "Add product id: $productId to delete batch", 2 );

                    // we have to deal with 2 versions of the API for now, one of which includes the version and one which doesn't
                    if( $includeVersion )
                    {
                        // create a SDF to delete the object from the index, including the version
                        $cloudSearchSDF[] = array(
                            'type'    => 'delete',
                            'id'      => $productId,
                            'version' => $timeNow
                        );
                    }
                    else
                    {
                        // create a SDF to delete the object from the index
                        $cloudSearchSDF[] = array(
                            'type' => 'delete',
                            'id'   => $productId
                        );
                    }

                    // set the last_indexed to 0
                    // get the underlying product data
                    if( $product = Product::model()
                                          ->findByPk( $productId )
                    )
                    {
                        $product->setAttribute( 'last_indexed', 0 );

                        // update the product with the last_indexed data
                        $product->save();
                    }

                }
            }
            // upload changes to CloudSearch
            if( $debug )
            {
                // if we're advanced debugging, return the SDF as we're not going to upload it
                if( $debug > 1 )
                {
                    return ( json_encode( $cloudSearchSDF ) );
                }
            }

            $this->debugMessage( $cloudSearchSDF );

            // get the CloudSearch object
            $cloudSearchObject = YiiItcher::app()->amazonCloudSearch;

            // if we don't have to include the version we need to set the new calendar method
            if( !$includeVersion )
            {
                $cloudSearchObject->setCalendarMethod( '2013-01-01' );
            }

            if( !empty( $cloudSearchSDF ) )
            {
                return $cloudSearchObject->document( $cloudSearchSDF, $this->_debug );
            }
        }

        return null;
    }

    /**
     * Function fills in the category tree for a set of categories.
     *
     * Also ensures each category has a name and a relevance ( relevance defaults to 0 )
     *
     * @param array $categoriesArray
     *
     * @return array
     */
    public function fillCategoriesArray( array $categoriesArray )
    {
        /**
         * we potentially call each of these several times so declare them outside the loop
         * @var CDbCommand $parentCategoryCommand
         * @var CDbCommand $categoryDetailsCommand
         */
        $parentCategoryCommand = $this->_db->createCommand( "SELECT parent_category_id FROM category WHERE category_id= :categoryId ;" );
        $categoryDetailsCommand = $this->_db->createCommand( "SELECT category_name FROM category WHERE category_id= :categoryId ;" );

        //for keeping track of what categories we have in the array
        $categoryIdsArray = array_column( $categoriesArray, 'category_id', 'category_id' );

        for( $i = 0; isset( $categoriesArray[$i] ); $i += 1 )
        {
            $categoryId = $categoriesArray[$i]['category_id'];
            $parentCacheKey = __METHOD__ . "::parentCategory::$categoryId";

            //get the parent category
            if( $parentId = $this->_cache->get( $parentCacheKey ) )
            {
                // use parent_category_id == category_id as a "no parent category" value
                if( $parentId != $categoryId && !isset( $categoryIdsArray[$parentId] ) )
                {
                    $categoriesArray[] = ['category_id' => $parentId];
                    $categoryIdsArray[$parentId] = $parentId;
                }
            }
            elseif( $parentId = $parentCategoryCommand->queryScalar( [':categoryId' => $categoryId] ) )
            {
                if( !isset( $categoryIdsArray[$parentId] ) )
                {
                    $categoriesArray[] = ['category_id' => $parentId];
                    $categoryIdsArray[$parentId] = $parentId;
                }
                $this->_cache->set( $parentCacheKey, $parentId, self::CACHE_TIME_ONE_WEEK );
            }
            else
            {
                // use parent_category_id == category_id as a "no parent category" value
                $this->_cache->set( $parentCacheKey, $categoryId, self::CACHE_TIME_ONE_WEEK );
            }

            // get the name if necessary
            if( empty( $categoriesArray[$i]['category_name'] ) )
            {
                $nameCacheKey = __METHOD__ . "::name::$categoryId";
                if( !( $name = $this->_cache->get( $nameCacheKey ) ) )
                {
                    $name = $categoryDetailsCommand->queryScalar( [':categoryId' => $categoryId] );
                }
                $categoriesArray[$i]['category_name'] = $name;
            }

            // default the relevance to 0 if we don't have it
            if( !isset( $categoriesArray[$i]['relevance'] ) )
            {
                $categoriesArray[$i]['relevance'] = 0;
            }
        }

        return $categoriesArray;
    }

    /**
     * Function strips invalid characters from AWS cloudsearch input
     *
     * @param string $string
     *
     * @return mixed
     */
    private function stripInvalidCodePoints( $string )
    {
        return preg_replace( "/[^\x{0009}\x{000a}\x{000d}\x{0020}-\x{D7FF}\x{E000}-\x{FFFD}]/u", '', (string)$string );
    }

    /**
     * Get the keywords for a product, attempt to get them through the cache first.
     *
     * @param      $productId
     * @param bool $resetCache
     *
     * @return array|CDbDataReader|mixed
     */
    public function getKeywordsThroughCache( $productId, $resetCache = false )
    {
        $cacheKey = "ProductKeywords::ProductId::$productId";

        // unless set not to attempt to get the keywords from the cache
        if( $resetCache or !$keywords = $this->cacheGet( $cacheKey ) )
        {
            $keywordsSQL = "SELECT keyword FROM product_keyword NATURAL JOIN keyword WHERE product_id = $productId;";

            // if we don't have keywords from the cache get them from the database
            $keywords = YiiItcher::app()->db->createCommand( $keywordsSQL )
                                            ->queryColumn();

            // add the keywords to the cache
            $this->cacheSet( $cacheKey, $keywords, self::$_CACHE_TIME_ONE_DAY );
        }

        return $keywords;
    }

    /**
     * Do an Amazon CloudSearch for the passed parameters
     *
     * @param int   $categoryId
     * @param array $params
     * @param bool  $debug
     *
     * @return array
     */
    public function cloudSearchByCategory( $categoryId, $params = array(), $debug = false )
    {
        $params['fq'] = "root_category_id:$categoryId";
        $results = json_decode( YiiItcher::app()->amazonCloudSearch->search( '-stringthatnoproductwilleverfeatureatallinordertogetallproductseventhoughamazonsaysnottodothis', $params, $debug ), true );
        if( $this->arrayCheck( $results, 'hits' ) )
        {
            $hits = $results['hits'];
            if( $this->arrayCheck( $hits, 'hit' ) )
            {
                return $hits['hit'];
            }
        }

        return array();
    }

    /**
     * Update the product_average_ratings and product_num_ratings fields for a
     * product
     *
     * @param integer $productId
     * @param bool    $updateCache
     *
     * @return array
     * @throws CDbException
     */
    public function updateProductReviewNumbers( $productId, $updateCache = true )
    {
        $response = array();

        /**
         * get the release_date for the product
         * @var $product Product
         */
        $product = Product::model()
                          ->findByPk( $productId );

        // get the average rating and number and variance of ratings for the product from the
        // ratings
        $ratingCalcSQL = "
			SELECT SUM( rating ) AS sum_rating, COUNT(*) AS num_ratings, VARIANCE( rating ) AS var_ratings
			FROM user_rating
			WHERE product_id=$productId;
		";

        $ratingCalc = YiiItcher::app()->db->createCommand( $ratingCalcSQL )
                                          ->queryRow();
        $averageRating = 0;
        $numRatings = 0;
        $varRatings = 0;
        if( $ratingCalc )
        {
            $varRatings = $ratingCalc['var_ratings'];
            $sumRating = $ratingCalc['sum_rating'];
            $numRatings = $ratingCalc['num_ratings'];
            if( $numRatings > 0 )
            {
                $averageRating = $sumRating / $numRatings;
            }
        }
        $response['product_id'] = $productId;
        $response['num_ratings'] = $numRatings;
        $response['var_ratings'] = $varRatings;
        $response['average_rating'] = $averageRating;

        // get the number of reviews for the product from the ratings
        $reviewsCalcSQL = "
			SELECT COUNT(DISTINCT(CONCAT(`name`, review_date, review_note))) AS num_reviews
			FROM user_review
			NATURAL JOIN `user`
			WHERE product_id=$productId;
		";

        $reviewsCalc = YiiItcher::app()->db->createCommand( $reviewsCalcSQL )
                                           ->queryRow();
        $numReviews = 0;
        if( $reviewsCalc )
        {
            $numReviews = $reviewsCalc['num_reviews'];
        }
        $response['num_reviews'] = $numReviews;

        // calculate the rank of the product
        // (variance+2)*(average+0.5)*log2(number+2)/log10(10000000+time in seconds since release)*100000
        $rank = RankService::create()
                           ->calculateRank( $productId, $numRatings, $averageRating, $varRatings, $product->release_date, $product->product_root_category_id );
        $response['rank'] = $rank;

        $weightedRating = 0;

        // if we have reviews or ratings
        if( $numReviews || $numRatings )
        {
            // calculate the weighted ratings
            if( $numRatings )
            {
                // the weighted rating is the average rating factored by the popularity
                $weightedRating = $averageRating + ( log( $numRatings ) / 2 );
                $response['weighted_rating'] = $weightedRating;
            }
        }
        $ratingUpdateSQL = "
            UPDATE product
            SET product_average_rating = " . $averageRating . ",
            product_weighted_rating = " . $weightedRating . ",
            product_num_ratings = " . $numRatings . ",
            product_num_reviews = " . $numReviews . ",
            rank = " . $rank . "
            WHERE product_id=$productId
        ";
        YiiItcher::app()->db->createCommand( $ratingUpdateSQL )
                            ->execute();

        // update the productElement cache if required
        if( $updateCache )
        {
            // queue the message for re-generation and re-caching
            MessageQueue::create()
                        ->queueMessage( self::$_PRODUCT_CACHE_QUEUE_NAME, $product['product_id'] );
        }

        // return the results
        return $response;
    }

    /**
     * Get the number of reviews for a products
     *
     * @param integer $productId
     *
     * @return integer
     */
    public function getNumReviewsForProduct( $productId )
    {
        // get the number of reviews for the product from the ratings
        $reviewsCalcSQL = "
			SELECT DISTINCT(CONCAT(`name`, review_date, review_note)) AS num_reviews
			FROM user_review
			NATURAL JOIN `user`
			WHERE product_id=$productId
		";

        $reviewsCalc = YiiItcher::app()->db->createCommand( $reviewsCalcSQL )
                                           ->queryRow();
        if( $reviewsCalc )
        {
            return $reviewsCalc['num_reviews'];
        }

        return 0;
    }

    /**
     * Get the set of products in a product list through the cache
     *
     * @param      $productList
     * @param      $offset
     * @param bool $precacheProducts - whether or not to pre-cache products with the FE fetch string
     *
     * @return array
     */
    public function getProductsFromProductList( $productList, $offset, $precacheProducts = false )
    {
        $this->deltaStartTimer( __FUNCTION__ );

        if( empty( $productList ) )
        {
            return [];
        }

        // get the required elements for the standard product listing
        $requiredElements = array(
            'product_id',
            'product_name',
            'root_category_id',
            'is_subproduct',
            'product_average_rating',
            'product_num_ratings',
            'product_num_reviews',
            'product_short_description',
            'release_date',
            'categories',
            'images',
            'ecommerce_product_links'
        );

        // if we're pre-caching products
        if( $precacheProducts )
        {
            $productOrder = array_flip( $productList );
            $products = [];
            $idsByPrecacheKey = [];
            foreach( $productList as $productId )
            {
                $precacheKey = "recentPrecache::$productId";
                $idsByPrecacheKey[$precacheKey] = $productId;
            }

            $recentPrecacheResult = $this->_cache->mget( array_keys( $idsByPrecacheKey ) );

            $recentlyPrecachedProductList = array_intersect_key( $idsByPrecacheKey, array_filter( $recentPrecacheResult ) );

            $nonPrecachedProductList = array_diff_key( $idsByPrecacheKey, $recentlyPrecachedProductList );

            // get products that have been precached already
            if( $recentlyPrecachedProductList )
            {
                foreach( $this->consolidateProductLookup( $recentlyPrecachedProductList, $requiredElements, 0, false, false, false ) as $recentlyPrecachedProduct )
                {
                    $products[$productOrder[$recentlyPrecachedProduct['product_id']]] = $recentlyPrecachedProduct;
                }
            }

            // precache and get products that haven't been precached yet
            if( $nonPrecachedProductList )
            {
                // add all the additional elements required for the product
                $requiredElements = array_merge( $requiredElements, array(
                        'product_short_description_length',
                        'product_description_length',
                        'all_images',
                        'related_items',
                    ) );

                foreach( $this->consolidateProductLookup( $nonPrecachedProductList, $requiredElements, 0, false, false, $precacheProducts ) as $nonPrecachedProduct )
                {
                    $products[$productOrder[$nonPrecachedProduct['product_id']]] = $nonPrecachedProduct;
                }

                RankService::create()
                           ->updateProducts( $nonPrecachedProductList );
                $this->_cache->msetex( array_fill_keys( array_keys( $nonPrecachedProductList ), true ), self::$_CACHE_TIME_ONE_HOUR * 12 );
            }

            // correct the product record_nums (necessary as we're joining 2 lists of product together)
            if( $products )
            {
                ksort( $products );
                $products = array_values( $products );
                foreach( $products as $productNum => $product )
                {
                    $products[$productNum]['record_num'] = $productNum + $offset;
                }
            }
        }
        else
        {
            // get the set of products
            $products = $this->consolidateProductLookup( $productList, $requiredElements, $offset, false, false, false );
        }

        // run through the products
        for( $productNum = 0, $count = count( $products ); $productNum < $count; $productNum++ )
        {
            // remove the subproducts if we have them
            if( isset( $products[$productNum]['product_subproducts'] ) )
            {
                unset( $products[$productNum]['product_subproducts'] );
            }
            // remove unwanted the attributes if we have them
            if( isset( $products[$productNum]['product_attributes'] ) )
            {
                $this->discardUnwantedAttributes( $products[$productNum]['product_attributes'], array(
                        'images',
                        'video_reviews',
                        'about',
                        'trailers',
                        'kindle_product_id',
                        'genre',
                        'related_user',
                        'info',
                        'artist',
                        'eisbn',
                        'news',
                        'other_albums',
                        'other_singles',
                        'album_id',
                        'single_id',
                        'spotify_uri',
                        'rotten_tomatoes_uri',
                        'urls',
                        'wikipedia',
                        'music_single',
                        'music_album',
                        'music_videos',
                        'editorial_review',
                        'product_short_description_length',
                        'product_description_length',
                        'all_images',
                        'related_items'
                    ) );
            }
        }

        $this->deltaEndTimer( __FUNCTION__ );

        // return the set of resolved products
        return $products;
    }

    /**
     * Provide a consolidated set of product data, including images and reviews
     *
     * @param array $productIDs
     * @param array $requiredElements
     * @param int   $offset
     * @param bool  $resetCache
     * @param bool  $assoc
     *            - return as an associated array
     * @param bool  $precacheProducts
     *
     * @return array product listing
     */
    public function consolidateProductLookup( $productIDs, $requiredElements, $offset, $resetCache = false, $assoc = false, $precacheProducts = false )
    {
        $this->deltaTimer( 'consolidateProductLookup' );
        $products = array();
        $productsAssoc = array();
        if( is_array( $productIDs ) && !empty( $productIDs ) )
        {
            // reconstitute the images as arrays and sub-arrays from the contents of the group_concat
            $productNum = 0;
            foreach( $productIDs as $product )
            {
                // put the product ids into an array if they are not already in one
                if( !is_array( $product ) )
                {
                    $product = array('product_id' => $product);
                }

                // get images and categories
                $this->getProductElementsThroughCache( $product, $requiredElements, $resetCache, $precacheProducts );

                // if we got a valid product set
                if( $product )
                {
                    $product['record_num'] = $offset + $productNum;

                    // merge the 2 arrays
                    // if we want to return an associative array create it here
                    if( $assoc )
                    {
                        $productsAssoc[$product['product_id']] = $product;
                    }
                    else
                    {
                        if( is_array( $product ) )
                        {
                            $products[$productNum] = $product;
                        }
                    }
                    $productNum++;
                }
            }
        }

        // if we've been asked to return an associative array
        if( $assoc )
        {
            // set the associative version
            $products = $productsAssoc;
        }

        $this->deltaTimer( 'consolidateProductLookup END' );

        // create and return merged data
        return $products;
    }

    /**
     * Function compares all of a products images against each other and updates the database to reflect the duplicates found.
     *
     * @param int                          $productId
     * @param int                          $customScoreThreshold
     * @param ImageDuplicationService|null $imageDuplicationService
     *
     * @return array|bool
     */
    public function updateProductImagesDuplication( $productId, $customScoreThreshold = null, ImageDuplicationService $imageDuplicationService = null )
    {
        // create data format for cache lookup
        $product = array('product_id' => $productId);

        // perform cache lookup (the fields we want are default return fields but list them to be safe)
        $this->getProductElementsThroughCache( $product, array('root_category_id', 'is_subproduct') );

        // if either desired field is missing (for archived products no fields will be added)
        if( !isset( $product['root_category_id'] ) or !isset( $product['is_subproduct'] ) )
        {
            // look them both up in the DB
            $productSql = "SELECT root_category_id, is_subproduct FROM product NATURAL JOIN category_product NATURAL JOIN category WHERE product_id = :productId;";
            $product = $this->_db->createCommand( $productSql )
                                 ->queryRow( true, array(':productId' => $productId) );
            unset( $productSql );
        }

        // if we found a root category id
        if( isset( $product['root_category_id'] ) && ( ( $rootCategoryId = (int)$product['root_category_id'] ) == $this->_booksRootCategoryId || $rootCategoryId == $this->_moviesRootCategoryId || $rootCategoryId == $this->_gamesRootCategoryId || $rootCategoryId == $this->_musicRootCategoryId ) )
        {
            // extract the data we want and discard the rest.
            if( !empty( $product['is_subproduct'] ) && $rootCategoryId == $this->_musicRootCategoryId )
            {
                $isSubproduct = true;
            }
            else
            {
                $isSubproduct = false;
            }
            unset( $product );

            $this->debugMessage( __METHOD__ . ": (productId:{$productId}) (rootCategoryId:{$rootCategoryId})" . ( $isSubproduct ? " (subproduct)" : '' ) );

            // set up a ImageDuplicationService object with the required properties (correct root_category_id and is_subproduct, most recent version, loaded dictionary)
            if( !isset( $imageDuplicationService ) )
            {
                // if we haven't been passed one, make a new one
                $imageDuplicationService = new ImageDuplicationService( $rootCategoryId, $isSubproduct );
            }
            else
            {
                // if we were passed one make sure it has the right settings (this will unload any dictionary it may have)
                $imageDuplicationService->setRootCategoryId( $rootCategoryId )
                                        ->setIsSubproduct( $isSubproduct );
            }

            if( $this->_debug )
            {
                $imageDuplicationService->setDebug( $this->_debug );
            }
            $imageDuplicationService->setToHighestSavedVersion()
                                    ->loadDictionary();

            // either use the supplied score threshold or load the default
            if( isset( $customScoreThreshold ) )
            {
                $imageDuplicationService->setScoreThreshold( $customScoreThreshold );
            }
            else
            {
                $imageDuplicationService->loadScoreThreshold();
            }

            // get a summary of the images for the product ( image url and image url md5 )
            $imagesSummary = $this->getProductImagesSummary( $productId, self::IMAGE_SUMMARY_OPTION_ASPECT_RATIO );

            $this->debugMessage( __METHOD__ . ": found " . count( $imagesSummary ) . " product images." );

            // get histograms for the images (computes them if necessary)
            $histogramsArray = $this->getProductImageHistogramsArray( $imageDuplicationService, $imagesSummary );

            // get an array of image ids of duplicate images
            $duplicateImageIdsArray = $this->getDuplicateImageIds( $imageDuplicationService, $histogramsArray );

            $this->debugMessage( __METHOD__ . ": found " . count( $duplicateImageIdsArray ) . " duplicate product images." );

            // mark each image as duplicate or not (attempt to set all images for the product).
            if( $changeCount = $this->markMultipleProductDuplicateImages( $productId, $duplicateImageIdsArray ) )
            {
                // if at least one image changed queue the product for caching
                $this->queueUncachedProduct( $productId );
            }
            $this->debugMessage( __METHOD__ . ": changed duplicate status for $changeCount product images." );

            return $duplicateImageIdsArray;
        }
        else
        {
            $this->debugMessage( __METHOD__ . ": no valid root category id" );

            return false;
        }
    }

    /**
     * Function gets some details about images for the product id given it, depending on the options specified.
     *
     * Gets id, url, md5(url) and area.
     *
     * @param int $inputId
     * @param int $options
     *
     * @return array
     */
    public function getProductImagesSummary( $inputId, $options = self::IMAGE_SUMMARY_OPTION_ALL )
    {
        switch( $options )
        {
            case self::IMAGE_SUMMARY_OPTION_ALL:
                // default (from the point of view of the function not the switch) to getting details for all images that were successfully downloaded (inputId is a product id)
                $imagesSql = "SELECT id AS image_id, image_url, image_url_md5, (image_width * image_height) AS image_size FROM product_image WHERE product_id = :productId AND downloaded = 1 AND download_error = 0;";

                $summary = $this->_db->createCommand( $imagesSql )
                                     ->queryAll( true, array(':productId' => $inputId) );
                break;

            case self::IMAGE_SUMMARY_OPTION_UNIQUE:
                // in this case get details for only the unique images (input id is a product id)
                $imagesSql = "SELECT id AS image_id, image_url, image_url_md5, (image_width * image_height) AS image_size FROM product_image WHERE product_id = :productId AND downloaded = 1 AND download_error = 0 AND product_image.duplicate = 0;";

                $summary = $this->_db->createCommand( $imagesSql )
                                     ->queryAll( true, array(':productId' => $inputId) );

                break;

            case self::IMAGE_SUMMARY_OPTION_SINGLE:
                // in this case get details for a single image (input id is an image id)
                $imagesSql = "SELECT id AS image_id, image_url, image_url_md5, (image_width * image_height) AS image_size FROM product_image WHERE product_image.id = :imageId;";

                $summary = $this->_db->createCommand( $imagesSql )
                                     ->queryAll( true, array(':imageId' => $inputId) );
                break;

            case self::IMAGE_SUMMARY_OPTION_ASPECT_RATIO:
                // default (from the point of view of the function not the switch) to getting details for all images that were successfully downloaded (inputId is a product id)
                $imagesSql = "SELECT id AS image_id, image_url, image_url_md5, (image_width * image_height) AS image_size, (image_width / image_height) AS aspect_ratio FROM product_image WHERE product_id = :productId AND downloaded = 1 AND download_error = 0;";

                $summary = $this->_db->createCommand( $imagesSql )
                                     ->queryAll( true, array(':productId' => $inputId) );
                break;

            default:
                throw new InvalidArgumentException( __METHOD__ . ": invalid option parameter" );
        }

        return $this->checkSummaryMd5s( $summary );
    }

    /**
     * Function checks that each entry in the image summary has an image_url_md5 and updates those that do not.
     *
     * @param array $imagesSummaryArray
     *
     * @return array
     */
    private function checkSummaryMd5s( array $imagesSummaryArray )
    {
        foreach( $imagesSummaryArray as &$imageSummary )
        {
            if( empty( $imageSummary['image_url_md5'] ) )
            {
                $imageSummary['image_url_md5'] = hex2bin( md5( $imageSummary['image_url'] ) );

                $this->debugMessage( __METHOD__ . ": updating image_url_md5 for image id:{$imageSummary['image_id']}" );

                $updateSQL = "UPDATE product_image SET image_url_md5 = :imageUrlMd5 WHERE id = :imageId;";

                $this->_db->createCommand( $updateSQL )
                          ->execute( array(':imageUrlMd5' => $imageSummary['image_url_md5'], ':imageId' => $imageSummary['image_id']) );
            }
        }

        return $imagesSummaryArray;
    }

    /**
     * Function gets histograms for each supplied image.
     *
     * @param ImageDuplicationService $dictionary
     * @param array                   $imagesSummary
     *
     * @return array
     */
    public function getProductImageHistogramsArray( ImageDuplicationService $dictionary, array $imagesSummary )
    {
        $histogramsArray = array();
        foreach( $imagesSummary as $imageDetails )
        {
            // we want the url md5 to be a hex string (for debugging mainly though I'm more comfortable with printable characters in general)
            $hexMd5 = bin2hex( $imageDetails['image_url_md5'] );

            //try and get a histogram
            if( $histogram = $this->loadOrComputeProductImageHistogram( $dictionary, $imageDetails['image_url'], $hexMd5 ) )
            {
                // if successful add it (and some extra information) to an array
                $histogramsArray[] = array(
                    'imageId'          => $imageDetails['image_id'],
                    'imageSize'        => $imageDetails['image_size'],
                    'imageAspectRatio' => $imageDetails['aspect_ratio'],
                    'histogram'        => $histogram
                );
            }
        }

        return $histogramsArray;
    }

    /**
     * Function tries to get a histogram for a given image url.
     *
     * It tries to create and save one if it can't find one to load.
     *
     * @param ImageDuplicationService $imageDictionary
     * @param string                  $imageUrl
     * @param string                  $imageUrlMd5
     *
     * @return bool|BowHistogram
     */
    public function loadOrComputeProductImageHistogram( ImageDuplicationService $imageDictionary, $imageUrl, $imageUrlMd5 )
    {
        // try to load it straight off
        try
        {
            return $imageDictionary->loadHistogram( $imageUrlMd5 );
        }
            //that throws a MissingHistogramException so catch it (discarding the exception)
        catch( MissingHistogramException $e )
        {
            unset( $e );

            // get the image from amazon S3
            $fileData = $this->getImageStreamFromS3( $imageUrl, $imageUrlMd5 );

            //if successful
            if( is_string( $fileData ) )
            {
                try
                {
                    // try to decode the image stream with openCV (it's more picky)
                    $image = OpenCvImage::decode( $fileData );
                }
                catch( InvalidImageException $e )
                {
                    // ignore the exception
                    unset( $e );

                    $this->debugMessage( __METHOD__ . ": Attempting to load unknown image type." );

                    // GD is less touchy about invalid images so try to create a GD image from the string, if successful
                    if( ( $GdImage = imagecreatefromstring( $fileData ) ) != false )
                    {
                        $this->debugMessage( __METHOD__ . ": Created GD image resource from image." );

                        // get a png style string from the image
                        ob_start();
                        imagepng( $GdImage );
                        $pngData = ob_get_contents();
                        ob_end_clean();

                        // destroy the GD image resource
                        imagedestroy( $GdImage );

                        // create the image form the new string
                        $image = OpenCvImage::decode( $pngData );
                    }
                }
            }

            // if no image was successfully loaded
            if( !isset( $image ) )
            {
                return false;
            }

            // extract the histogram from the image
            $histogram = $imageDictionary->extractHistogram( $image, $imageUrlMd5 );

            // save the histogram
            $imageDictionary->saveHistogram( $histogram );

            return $histogram;
        }
    }

    /**
     * Gets a string representation of an image from amazon s3.
     *
     * @param string $imageUrl
     * @param string $imageUrlMd5
     *
     * @return string
     */
    public function getImageStreamFromS3( $imageUrl, $imageUrlMd5 )
    {
        // calculate the file name
        $s3Filename = str_replace( '+', '_', strtolower( substr( strrchr( $imageUrl, '/' ), 1 ) ) );
        $destinationFilename = 'products/original/' . $imageUrlMd5 . '/' . $s3Filename;

        // get the image from s3
        return YiiItcher::app()->amazonS3->getFileData( $destinationFilename );
    }

    /**
     * Function goes through an array of image histograms and returns an array of image ids of those images that are duplicates
     *
     * @param ImageDuplicationService $imageDuplicationService
     * @param array                   $histogramsArray
     *
     * @return array
     */
    public function getDuplicateImageIds( ImageDuplicationService $imageDuplicationService, array $histogramsArray )
    {
        $imageCount = count( $histogramsArray );
        $duplicateImageIdsArray = array();

        // nested for loops to compare each pair of histograms at most once
        for( $i = 1; $i < $imageCount; $i += 1 )
        {
            for( $j = 0; $j < $i; $j += 1 )
            {
                // if the histogram still exists (histograms get dropped if they're duplicates so we stop comparing against them)
                // and the aspect ratios are close enough
                // and the histograms match
                if( isset( $histogramsArray[$j] ) && $this->compareAspectRatios( $histogramsArray[$i]['imageAspectRatio'], $histogramsArray[$j]['imageAspectRatio'] ) && $imageDuplicationService->evaluateHistograms( $histogramsArray[$i]['histogram'], $histogramsArray[$j]['histogram'] ) )
                {
                    //if we've already found a match for the outer loop histogram
                    if( isset( $matchKey ) )
                    {
                        // then it's a duplicate, add it to the array and move on to the next (outer loop) one
                        $duplicateImageIdsArray[] = $histogramsArray[$i]['imageId'];
                        unset( $matchKey, $histogramsArray[$i] );
                        break;
                    }
                    else
                    {
                        // save the array key of the {inner loop histogram} and keep looking
                        $matchKey = $j;
                    }
                }
            }

            if( isset( $matchKey ) )
            {
                // if we found a single match mark the smaller image (the {outer loop image} in the case of a tie) as a duplicate
                if( $histogramsArray[$matchKey]['imageSize'] >= $histogramsArray[$i]['imageSize'] )
                {
                    $duplicateImageIdsArray[] = $histogramsArray[$i]['imageId'];
                    unset( $matchKey, $histogramsArray[$i] );
                }
                else
                {
                    $duplicateImageIdsArray[] = $histogramsArray[$matchKey]['imageId'];
                    unset( $histogramsArray[$matchKey], $matchKey );
                }
            }
        }

        return $duplicateImageIdsArray;
    }

    /**
     * Checks that the aspect ratios are sufficiently similar
     *
     * @param int $aspectRatioAlpha
     * @param int $aspectRatioBeta
     *
     * @return bool
     */
    private function compareAspectRatios( $aspectRatioAlpha, $aspectRatioBeta )
    {
        $this->debugMessage( __METHOD__ . ": alpha:$aspectRatioAlpha\tbeta:$aspectRatioBeta", 2 );

        // calculate a symmetric comparison value
        $comparisonValue = $aspectRatioAlpha / $aspectRatioBeta + $aspectRatioBeta / $aspectRatioAlpha;

        // compare the value to the maximum limit.
        return $comparisonValue < self::ASPECT_RATIO_COMPARISON_LIMIT;
    }

    /**
     * Function updates all images of a given product to be either duplicate or not based on a supplied list of duplicate image ids.
     *
     * @param int   $productId
     * @param array $duplicateImageIdsArray
     *
     * @return int
     */
    public function markMultipleProductDuplicateImages( $productId, array $duplicateImageIdsArray )
    {
        // as there are a variable number of duplicate images and we want to use a prepared statement to access the DB building the query is a little complex
        $duplicateImageCount = count( $duplicateImageIdsArray );
        $parameters = array(':productId' => $productId);

        if( $duplicateImageCount > 0 )
        {
            // if there are any duplicates we need a place for each to bind to and a entry for each in the parameters list.

            // will become a CSV list in the query using implode
            $bindingsArray = array();
            for( $i = 0; $i < $duplicateImageCount; $i += 1 )
            {
                $parameters[':imageId' . $i] = $duplicateImageIdsArray[$i];
                $bindingsArray[] = ':imageId' . $i;
            }

            // construct the query
            $updateImagesSql = "UPDATE product_image SET product_image.duplicate = IF( product_image.id IN(" . implode( ',', $bindingsArray ) . "), 1, 0 ) WHERE product_image.product_id = :productId AND product_image.download_error = 0 AND product_image.downloaded = 1;";
        }
        else
        {
            // if there are no duplicates the query is simple
            $updateImagesSql = "UPDATE product_image SET product_image.duplicate = 0 WHERE product_image.product_id = :productId AND product_image.download_error = 0 AND product_image.downloaded = 1;";
        }

        // create and execute the command
        return $this->_db->createCommand( $updateImagesSql )
                         ->execute( $parameters );
    }

    /**
     * Queue product to product cache queue if not currently in the cache
     *
     * @param      $productId
     * @param bool $ignoreCache
     *
     * @throws Exception
     */
    public function queueUncachedProduct( $productId, $ignoreCache = false )
    {
        // if we don't have the item in cache or we're just queueing to re-cache
        if( !$this->checkRedisCachedObject( 'product', $productId ) || $ignoreCache )
        {
            // queue the message for re-caching
            MessageQueue::create()
                        ->queueMessage( self::$_PRODUCT_CACHE_QUEUE_NAME, $productId );
        }
    }

    /**
     * Method goes over all of a product's images and checks if they match any restricted images.
     *
     * Not the most efficient method in the world, only designed to be run once per product.
     *
     * @param $productId
     *
     * @return array|bool
     */
    public function updateProductImagesRestriction( $productId )
    {
        // create data format for cache lookup
        $product = array('product_id' => $productId);

        // perform cache lookup (the field we want is a default return field but list it to be safe)
        $this->getProductElementsThroughCache( $product, array('root_category_id') );

        // if the desired field is missing (for archived products no fields will be added)
        if( !isset( $product['root_category_id'] ) or !isset( $product['is_subproduct'] ) )
        {
            // look them both up in the DB
            $productSql = "SELECT root_category_id FROM product NATURAL JOIN category_product NATURAL JOIN category WHERE product_id = :productId;";
            $product = $this->_db->createCommand( $productSql )
                                 ->queryRow( true, array(':productId' => $productId) );
            unset( $productSql );
        }

        // if we found a root category id
        if( isset( $product['root_category_id'] ) && ( ( $rootCategoryId = (int)$product['root_category_id'] ) == $this->_booksRootCategoryId || $rootCategoryId == $this->_moviesRootCategoryId || $rootCategoryId == $this->_gamesRootCategoryId || $rootCategoryId == $this->_musicRootCategoryId ) )
        {
            unset( $product );

            $this->debugMessage( __METHOD__ . ": (productId:{$productId}) (rootCategoryId:{$rootCategoryId})" );

            // get the information about the restricted images to compare against
            $imageDownloadService = new ImageDownloadService();
            $restrictedImagesArray = $imageDownloadService->getRestrictedImagesThroughCache( $rootCategoryId );

            if( !empty( $restrictedImagesArray ) )
            {
                $this->debugMessage( __METHOD__ . ": found " . count( $restrictedImagesArray ) . " restricted image types." );

                // get a summary of the images for the product ( image url and image url md5 )
                $imagesSummary = $this->getProductImagesSummary( $productId );

                $this->debugMessage( __METHOD__ . ": found " . count( $imagesSummary ) . " product images." );

                // get a gd image resource for each image
                $gdImages = $this->loadGdImagesArray( $imagesSummary );

                $this->debugMessage( __METHOD__ . ": loaded " . count( $gdImages ) . " product images successfully." );

                // get the image ids of the restricted images
                $restrictedImageIdsArray = $this->computeProductRestrictedImagesArray( $gdImages, $rootCategoryId, $restrictedImagesArray, $imageDownloadService );

                $this->debugMessage( __METHOD__ . ": found " . count( $restrictedImageIdsArray ) . " restricted images for the product." );

                // mark each restricted image as having a download error
                if( $changeCount = $this->markProductRestrictedImages( $productId, $restrictedImageIdsArray ) )
                {
                    // if at least one image changed queue the product for caching
                    $this->queueUncachedProduct( $productId );
                }
                $this->debugMessage( __METHOD__ . ": marked $changeCount restricted images." );

                return $restrictedImageIdsArray;
            }
            else
            {
                $this->debugMessage( __METHOD__ . ": no restricted image types for this root category id: {$rootCategoryId}" );

                return array();

            }
        }
        else
        {
            $this->debugMessage( __METHOD__ . ": no valid root category id" );

            return false;
        }
    }

    /**
     * Method loads images from amazonS3 as gd image resources
     *
     * @param array $imageSummariesArray
     *
     * @return array
     */
    public function loadGdImagesArray( array $imageSummariesArray )
    {
        $gdImagesArray = array();

        // for each image
        foreach( $imageSummariesArray as $imageSummary )
        {
            // try to get it from s3
            $imageStream = $this->getImageStreamFromS3( $imageSummary['image_url'], bin2hex( $imageSummary['image_url_md5'] ) );

            // if it worked
            if( $imageStream )
            {
                // create the image resource (store the id in the key)
                $gdImagesArray["id:{$imageSummary['image_id']}"] = imagecreatefromstring( $imageStream );
            }
        }

        return $gdImagesArray;
    }

    /**
     * Method check each image against the restricted images for the root category
     *
     * @param array                $imagesArray
     * @param int                  $rootCategoryId
     * @param array                $restrictedImagesArray
     * @param ImageDownloadService $imageDownloadService
     *
     * @return array
     */
    public function computeProductRestrictedImagesArray( array $imagesArray, $rootCategoryId, array $restrictedImagesArray = null, ImageDownloadService $imageDownloadService = null )
    {
        // if weren't passed these things then look them up
        if( !isset( $imageDownloadService ) )
        {
            $imageDownloadService = new ImageDownloadService();
        }
        if( !isset( $restrictedImagesArray ) )
        {
            $restrictedImagesArray = $imageDownloadService->getRestrictedImagesThroughCache( $rootCategoryId );
        }

        $restrictedImageIdsArray = array();

        // for each image
        foreach( $imagesArray as $prefixedImageId => $image )
        {
            // test it against the restricted images
            if( $imageDownloadService->computeIsImageRestricted( $image, $rootCategoryId, $restrictedImagesArray ) )
            {
                // if it was a match add it to the list of restricted images
                $restrictedImageIdsArray[] = substr( $prefixedImageId, 3 );
            }
        }

        return $restrictedImageIdsArray;
    }

    /**
     * Method marks restricted images as having download errors (which excludes them from display etc.)
     *
     * @param int   $productId
     * @param array $imageIdsArray
     *
     * @return int
     * @throws CDbException
     */
    public function markProductRestrictedImages( $productId, array $imageIdsArray )
    {
        // as there are a variable number of duplicate images and we want to use a prepared statement to access the DB building the query is a little complex
        $restrictedImageCount = count( $imageIdsArray );

        if( $restrictedImageCount > 0 )
        {
            // if there are any restricted images we need a place for each to bind to and a entry for each in the parameters list.
            $parameters = array(':productId' => $productId);

            // will become a CSV list in the query using implode
            $bindingsArray = array();
            for( $i = 0; $i < $restrictedImageCount; $i += 1 )
            {
                $parameters[':imageId' . $i] = $imageIdsArray[$i];
                $bindingsArray[] = ':imageId' . $i;
            }

            // construct the query
            $updateImagesSql = "UPDATE product_image SET product_image.download_error = 1 WHERE product_image.product_id = :productId AND product_image.id IN(" . implode( ',', $bindingsArray ) . ");";

            // create and execute the command, returning the number of rows changed
            return $this->_db->createCommand( $updateImagesSql )
                             ->execute( $parameters );
        }
        else
        {
            return 0;
        }

    }

    /**
     * Function compares one image to all other images not already marked as duplicate for a given product
     *
     * @param int    $imageId
     * @param string $imageUrl
     * @param int    $productId
     * @param int    $customScoreThreshold
     *
     * @return int|bool
     */
    public function checkImageDuplicationWithProductImages( $imageId, $imageUrl, $productId, $customScoreThreshold = null )
    {
        // check the image is for the product
        if( !$this->checkImageMatchesProduct( $imageId, $productId ) )
        {
            // todo: create a read only version for if the image is for a different product. (might be useful for product de-duping)
            throw new InvalidArgumentException( __METHOD__ . "; image must be for product supplied" );
        }

        // get the details about the product that we need
        $product = array('product_id' => $productId);
        $this->getProductElementsThroughCache( $product, array('root_category_id') );
        if( !isset( $product['root_category_id'] ) or !isset( $product['is_subproduct'] ) )
        {
            $productSql = "SELECT root_category_id, is_subproduct FROM product NATURAL JOIN category_product NATURAL JOIN category WHERE product_id = :productId;";
            $product = $this->_db->createCommand( $productSql )
                                 ->queryRow( true, array(':productId' => $productId) );
            unset( $productSql );
        }

        // if we got a root category id
        if( isset( $product['root_category_id'] ) && ( ( $rootCategoryId = (int)$product['root_category_id'] ) == $this->_booksRootCategoryId || $rootCategoryId == $this->_moviesRootCategoryId || $rootCategoryId == $this->_gamesRootCategoryId || $rootCategoryId == $this->_musicRootCategoryId ) )
        {
            // md5 the image url
            $imageUrlMd5 = md5( $imageUrl );

            //extract the data we need from the data we got
            $product['root_category_id'];
            if( !empty( $product['is_subproduct'] ) && $rootCategoryId == $this->_musicRootCategoryId )
            {
                $isSubproduct = true;
            }
            else
            {
                $isSubproduct = false;
            }
            unset( $product );

            // set up image duplication service.
            $imageDuplicationService = new ImageDuplicationService( $rootCategoryId, $isSubproduct );
            if( $this->_debug )
            {
                $imageDuplicationService->setDebug( $this->_debug );
            }
            $imageDuplicationService->setToHighestSavedVersion()
                                    ->loadDictionary();
            if( isset( $customScoreThreshold ) )
            {
                $imageDuplicationService->setScoreThreshold( $customScoreThreshold );
            }
            else
            {
                $imageDuplicationService->loadScoreThreshold();
            }

            // get image summary for non duplicate images
            $imagesSummary = $this->getProductImagesSummary( $productId, self::IMAGE_SUMMARY_OPTION_UNIQUE );

            $this->debugMessage( __METHOD__ . ": Found " . count( $imagesSummary ) . " non-duplicate product images." );

            // get the histograms for the images
            $histogramsArray = $this->getProductImageHistogramsArray( $imageDuplicationService, $imagesSummary );

            // isolate the histogram for the input image
            foreach( $histogramsArray as $key => $histogram )
            {
                if( $histogram['imageId'] == $imageId )
                {
                    $imageHistogram = $histogram;
                    array_splice( $histogramsArray, $key, 1 );
                    break;
                }
            }

            // if we didn't find the histogram for the input image then get it specifically
            if( !isset( $imageHistogram ) )
            {
                $imageDetails = $this->getProductImagesSummary( $imageId, self::IMAGE_SUMMARY_OPTION_SINGLE );
                $imageHistogram = array(
                    'histogram' => $this->loadOrComputeProductImageHistogram( $imageDuplicationService, $imageUrl, $imageUrlMd5 ),
                    'imageId'   => $imageId,
                    'imageSize' => $imageDetails['image_size']
                );
                unset( $imageDetails );
            }

            // set this to null so we can use it later if it doesn't get overwritten
            $duplicateImageId = null;

            // for each histogram other than the one for the input image
            foreach( $histogramsArray as $key => $currentHistogram )
            {
                // compare it to the input histogram
                if( $imageDuplicationService->evaluateHistograms( $currentHistogram['histogram'], $imageHistogram['histogram'] ) )
                {
                    //if we find a match
                    if( isset( $matchKey ) )
                    {
                        // if we have previously found a match, then just mark the input image as a duplicate and end the loop
                        $duplicateImageId = $currentHistogram['imageId'];
                        unset( $matchKey );
                        break;
                    }
                    else
                    {
                        // save the key of the match
                        $matchKey = $key;
                    }
                }
            }

            // if we have a single match
            if( isset( $matchKey ) )
            {
                // mark the smaller image as a duplicate (mark the input image if they're equal)
                if( $histogramsArray[$matchKey]['imageSize'] >= $imageHistogram['imageSize'] )
                {
                    $duplicateImageId = $imageHistogram['imageId'];
                    unset( $matchKey );
                }
                else
                {
                    $duplicateImageId = $histogramsArray[$matchKey]['imageId'];
                    unset( $matchKey );
                }
            }

            // update the product images correctly (changes at most 2 images, at most one of which will be marked as a duplicate)
            if( $this->markLimitedProductDuplicateImages( $imageId, $duplicateImageId ) )
            {
                // if at least one image changed queue the product for re-caching
                $this->queueUncachedProduct( $productId );
            }
        }

        if( isset( $duplicateImageId ) )
        {
            return $duplicateImageId;
        }
        else
        {
            return false;
        }
    }

    /**
     * Simple check that the image id refers to an image for the product id.
     *
     * I decided that this is unlikely to get enough repeat calls (same image and product) to be worth caching.
     *
     * @param int $imageId
     * @param int $productId
     *
     * @return bool
     */
    public function checkImageMatchesProduct( $imageId, $productId )
    {
        // query returns 1 or false
        $checkSQL = "SELECT 1 FROM product_image WHERE product_id = :productId AND product_image.id = :imageId;";

        // cast the 1 to true
        return (bool)$this->_db->createCommand( $checkSQL )
                               ->queryScalar( array(':productId' => $productId, ':imageId' => $imageId) );
    }

    /**
     * Function updates the input image as either a duplicate or not and possibly updates a second image as a duplicate
     *
     * @param int $imageId
     * @param int $duplicateImageId
     *
     * @return int|bool
     */
    public function markLimitedProductDuplicateImages( $imageId, $duplicateImageId = null )
    {
        $params = array(':imageId' => $imageId);
        if( isset( $duplicateImageId ) )
        {
            // if we found a duplicate
            if( $duplicateImageId == $imageId )
            {
                // and it was the input image, mark the input image as a duplicate
                $this->debugMessage( __METHOD__ . ": supplied image is a duplicate image" );
                $updateImagesSql = "UPDATE product_image SET product_image.duplicate = 1 WHERE product_image.id = :imageId;";
            }
            else
            {
                // and it wasn't the input image mark that image as a duplicate and the input image as a not-duplicate
                $this->debugMessage( __METHOD__ . ": found a single smaller duplicate of supplied image" );
                $updateImagesSql = "UPDATE product_image SET product_image.duplicate = IF(product_image.id = :duplicateImageId, 1, 0) WHERE product_image.id IN( :imageId, :duplicateImageId);";
                $params[':duplicateImageId'] = $duplicateImageId;
            }
        }
        else
        {
            // mark the input image as a not-duplicate
            $this->debugMessage( __METHOD__ . ": found no duplicates of supplied image" );
            $updateImagesSql = "UPDATE product_image SET product_image.duplicate = 0 WHERE product_image.id = :imageId;";
        }

        return $this->_db->createCommand( $updateImagesSql )
                         ->execute( $params );
    }

    /**
     * Create e-commerce links for a product
     *
     * @param array $product
     * @param bool  $forceUpdate
     *
     * @throws Exception
     */
    public function createProductEcommerceLinks( $product, $forceUpdate = false )
    {
        // quick hack to seed ecommerce products for books
        $booksId = null;
        $amazonDataProviderId = null;
        $productDuplicationDataProviderId = null;
        $ecommerceProductId = null;
        $amazonProductId = null;
        $amazonDataProvider = null;
        $directAmazonProductLink = null;
        $productName = null;
        $productPrice = null;
        $productPriceUS = null;
        $productThumbnail = null;

        // get the product ID
        $productId = $product['product_id'];

        // if there isn't a root category in the cached producy
        if( !isset( $product['root_category_id'] ) )
        {
            // get the root_category_id from the underlying product
            $product['root_category_id'] = Product::model()
                                                  ->getRootCategoryId( $productId );
        }
        // if it's a book
        if( $product['root_category_id'] == $this->booksRootId() )
        {
            // if we haven't already processed this product
            $cacheKey = "ecommerce_product_id_4: $productId";
            if( $forceUpdate or !( $ecommerceProductId = $this->cacheGet( $cacheKey ) ) )
            {
                /**
                 * get the data_provider_id for amazon
                 * @var $dataProvider DataProvider
                 */
                $dataProvider = DataProvider::model()
                                            ->findByAttributes( array(
                                                    'data_provider_name' => 'amazon'
                                                ) );
                $amazonDataProviderId = $dataProvider->data_provider_id;

                // get a handle to the amazon data provider for the UK
                $amazonDataProvider = new AmazonDataProvider();
                $amazonDataProvider->init( 'GB' );

                $productDuplicationDataProvider = new ProductDuplicationDataProvider();
                $productDuplicationDataProvider->init( 'GB' );

                if( $this->_debug )
                {
                    $productDuplicationDataProvider->setDebug( $this->_debug );
                    $amazonDataProvider->setDebug( $this->_debug );
                }

                $this->debugMessage( "Lookup ecommerce product for $productId" );

                // see if there is a direct amazon DP record for this product
                $directAmazonProduct = $this->_db->createCommand( "SELECT data_provider_product_id, product_name, product_link, product_attributes FROM data_provider_product WHERE product_id = $productId AND data_provider_id = $amazonDataProviderId" )
                                                 ->queryRow();

                // if there isn't or if the product is a master product
                if( !$directAmazonProduct or $productDuplicationDataProvider->isMasterProduct( $productId ) )
                {
                    // load the data provider product
                    /** @var DataProviderProduct $productDuplicationDataProviderProduct */
                    $productDuplicationDataProviderProduct = DataProviderProduct::model()
                                                                                ->findByAttributes( array(
                                                                                        'product_id'       => $productId,
                                                                                        'data_provider_id' => $productDuplicationDataProvider->getDataProviderId()
                                                                                    ) );

                    // don't process any product which has been deleted and is only here because it's still in the cache
                    if( !$productDuplicationDataProviderProduct )
                    {
                        return;
                    }

                    // if it's a master product
                    if( $productDuplicationDataProviderProduct->product_name == ProductDuplicationDataProvider::MASTER_PRODUCT_NAME )
                    {
                        // get the product attributes
                        $subordinateProducts = json_decode( $productDuplicationDataProviderProduct->product_attributes, true );

                        // if we have some product ids
                        if( isset( $subordinateProducts[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) )
                        {
                            // get them and put them into a list
                            $subProductIds = '';
                            if( count( $subordinateProducts[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] ) > 0 )
                            {
                                // for each subordinate product
                                foreach( $subordinateProducts[ProductDuplicationDataProvider::MASTER_SUBORDINATE_PRODUCT_IDS_KEY] as $subordinateProductId )
                                {
                                    // check the relationship
                                    if( $productDuplicationDataProvider->checkDuplicateRelationship( $productDuplicationDataProviderProduct, $subordinateProductId, ProductDuplicationDataProvider::SUBORDINATE_MASTER_RELATIONSHIP ) )
                                    {
                                        // if it exists correctly, add the id to the CSV list
                                        if( !empty( $subProductIds ) )
                                        {
                                            $subProductIds .= ',';
                                        }
                                        $subProductIds .= $subordinateProductId;
                                    }
                                    else
                                    {
                                        // if the relationship is broken, queue it for repair
                                        $productDuplicationDataProvider->repairDuplicateRelationshipsQueued( array('product_id' => $productId, 'repair' => true), true );
                                    }
                                }
                            }

                            // see if we can get a direct link from one of the subordinate products
                            if( !empty( $subProductIds ) )
                            {
                                // see if there is a direct amazon DP record for this product
                                $directAmazonProduct = $this->_db->createCommand( "SELECT data_provider_product_id, product_link, product_name, product_attributes, product_id FROM data_provider_product WHERE product_id in ($subProductIds) AND data_provider_id = $amazonDataProviderId" )
                                                                 ->queryRow();

                                // if so
                                if( !empty( $directAmazonProduct ) )
                                {
                                    $this->debugMessage( "Found subordinate ecommerce product for $productId" );
                                    // setup the data from Amazon
                                    $ecommerceProductId = $productId;
                                    $chosenSubordinateProductId = $directAmazonProduct['product_id'];
                                    $directAmazonProductLink = $directAmazonProduct['product_link'];
                                    $amazonProductId = $directAmazonProduct['data_provider_product_id'];
                                }
                            }
                        }
                    }
                }
                else
                {
                    $this->debugMessage( "Found direct ecommerce product for $productId" );
                    // setup the data from Amazon
                    $ecommerceProductId = $productId;
                    $directAmazonProductLink = $directAmazonProduct['product_link'];
                    $amazonProductId = $directAmazonProduct['data_provider_product_id'];
                }

                // get additional e-commerce attributes
                $productName = $directAmazonProduct['product_name'];
                $productAttributes = json_decode( $directAmazonProduct['product_attributes'], true );

                // add in price amount
                if( isset( $productAttributes['ListPrice']['Amount'] ) )
                {
                    $productPrice = floatval( $productAttributes['ListPrice']['Amount'] / 100 );
                    $productPriceUS = floatval( $productAttributes['ListPrice']['Amount'] / 75 );
                }
                elseif( isset( $item['OfferSummary']['LowestNewPrice']['Amount'] ) )
                {
                    $productPrice = floatval( $item['OfferSummary']['LowestNewPrice']['Amount'] / 100 );
                    $productPriceUS = floatval( $item['OfferSummary']['LowestNewPrice']['Amount'] / 75 );
                }
                elseif( isset( $item['OfferSummary']['LowestUsedPrice']['Amount'] ) )
                {
                    $productPrice = floatval( $item['OfferSummary']['LowestUsedPrice']['Amount'] / 100 );
                    $productPriceUS = floatval( $item['OfferSummary']['LowestUsedPrice']['Amount'] / 75 );
                }
                $productImages = $this->getProductAttributesByProductId( null, $product['product_id'], array('primary_portrait_image', 'primary_square_image') );
                if( isset( $productImages['primary_portrait_image'] ) )
                {
                    if( is_array( $productImages['primary_portrait_image'] ) && isset( $productImages['primary_portrait_image']['image_url'] ) )
                    {
                        $productThumbnail = $productImages['primary_portrait_image']['image_url'];
                    }
                    else
                    {
                        $productThumbnail = $productImages['primary_portrait_image'];
                    }
                }
                elseif( isset( $productImages['primary_square_image'] ) )
                {
                    if( is_array( $productImages['primary_square_image'] ) && isset( $productImages['primary_square_image']['image_url'] ) )
                    {
                        $productThumbnail = $productImages['primary_square_image']['image_url'];
                    }
                    else
                    {
                        $productThumbnail = $productImages['primary_square_image'];
                    }
                }

                // if we got an ecommerce product id
                if( $ecommerceProductId )
                {
                    /**
                     * decode the amazon link to validate it is one
                     * http://www.amazon.co.uk/Brief-History-Time-Stephen-Hawking/dp/1439503923%3FSubscriptionId%3DAKIAJXHFTSF7DAS5HWGA%26tag%3Dgreminthiali-21%26linkCode%3Dsp1%26camp%3D2025%26creative%3D165953%26creativeASIN%3D1439503923
                     */
                    $linkBits = explode( '/', $directAmazonProductLink );

                    // if we got anough to parse out their codes
                    if( count( $linkBits ) == 6 )
                    {
                        /**
                         * get their codes bit which should be a url encoded link
                         * 1439503923%3FSubscriptionId%3DAKIAJXHFTSF7DAS5HWGA%26tag%3Dgreminthiali-21%26linkCode%3Dsp1%26camp%3D2025%26creative%3D165953%26creativeASIN%3D1439503923
                         */
                        $url = urldecode( $linkBits[5] );

                        /**
                         * we should have a bunch of querystring params after the page marker
                         * 1439503923?SubscriptionId=AKIAJXHFTSF7DAS5HWGA&tag=greminthiali-21&linkCode=sp1&camp=2025&creative=165953&creativeASIN=1439503923
                         */
                        $qsBits = explode( '?', $url );

                        /**
                         * if so
                         * SubscriptionId=AKIAJXHFTSF7DAS5HWGA&tag=greminthiali-21&linkCode=sp1&camp=2025&creative=165953&creativeASIN=1439503923
                         */
                        if( count( $qsBits ) > 1 )
                        {
                            // parse it
                            parse_str( $qsBits[1], $urlBits );

                            /**
                             * we should have SubscriptionId and tag as 2 of the params
                             * Array
                             * (
                             * [SubscriptionId] => AKIAJUBRG54RJKWMONCQ
                             * [tag] => itcher-20
                             * [linkCode] => sp1
                             * [camp] => 2025
                             * [creative] => 165953
                             * [creativeASIN] => 185715035X
                             * )
                             */
                            if( isset( $urlBits['SubscriptionId'] ) && isset( $urlBits['tag'] ) )
                            {
                                // if we have valid config info for each country
                                $amazonEcsConfig = $amazonDataProvider->getConfigInfo();
                                if( isset( $amazonEcsConfig['UK'] ) && isset( $amazonEcsConfig['US'] ) )
                                {
                                    // sub out any invalid parameters
                                    $directAmazonUkProductLink = str_replace( 'amazon.com', 'amazon.co.uk', str_replace( $urlBits['SubscriptionId'], $amazonEcsConfig['UK']['accessKey'], str_replace( $urlBits['tag'], $amazonEcsConfig['UK']['associateTag'], $directAmazonProductLink ) ) );
                                    $directAmazonUsProductLink = str_replace( 'amazon.co.uk', 'amazon.com', str_replace( $urlBits['SubscriptionId'], $amazonEcsConfig['US']['accessKey'], str_replace( $urlBits['tag'], $amazonEcsConfig['US']['associateTag'], $directAmazonProductLink ) ) );
                                }
                            }
                        }
                    }

                    // if we have a valid amazon product link
                    if( isset( $directAmazonUkProductLink ) )
                    {
                        // save the Amazon UK link
                        $amazonDataProvider->saveEcommerceProduct( $ecommerceProductId, $directAmazonUkProductLink, $productName, $productThumbnail, $productPrice );

                        // if we got the link from a subordinate
                        if( isset( $chosenSubordinateProductId ) )
                        {
                            // save the Amazon UK link for the subordinate too.
                            $amazonDataProvider->saveEcommerceProduct( $chosenSubordinateProductId, $directAmazonUkProductLink, $productName, $productThumbnail, $productPrice );
                        }
                    }
                    else
                    {
                        // queue it for lookup for the UK
                        $amazonDataProvider->getEcommerceLinkByProductIdByCountryQueued( array(
                            'product_id'               => $ecommerceProductId,
                            'data_provider_product_id' => $amazonProductId,
                            'country_code'             => 'UK'
                        ), true );

                        // if we got the id from a subordinate
                        if( isset( $chosenSubordinateProductId ) )
                        {
                            // queue the subordinate for lookup for the UK too
                            $amazonDataProvider->getEcommerceLinkByProductIdByCountryQueued( array(
                                'product_id'   => $chosenSubordinateProductId,
                                'country_code' => 'UK'
                            ), true );
                        }
                    }

                    // if we have a valid amazon product link
                    if( isset( $directAmazonUsProductLink ) )
                    {
                        // save the Amazon UK link
                        $amazonDataProvider->saveEcommerceProduct( $ecommerceProductId, $directAmazonUsProductLink, $productName, $productThumbnail, $productPriceUS, 'US' );

                        // if we got the link from a subordinate
                        if( isset( $chosenSubordinateProductId ) )
                        {
                            // save the Amazon UK link for the subordinate too.
                            $amazonDataProvider->saveEcommerceProduct( $chosenSubordinateProductId, $directAmazonUsProductLink, $productName, $productThumbnail, $productPriceUS, 'US' );
                        }
                    }
                    else
                    {
                        // queue it for lookup for the US
                        $amazonDataProvider->getEcommerceLinkByProductIdByCountryQueued( array(
                            'product_id'               => $ecommerceProductId,
                            'data_provider_product_id' => $amazonProductId,
                            'country_code'             => 'US'
                        ), true );

                        // if we got the id from a subordinate
                        if( isset( $chosenSubordinateProductId ) )
                        {
                            // queue the subordinate for lookup for the US too
                            $amazonDataProvider->getEcommerceLinkByProductIdByCountryQueued( array(
                                'product_id'   => $chosenSubordinateProductId,
                                'country_code' => 'US'
                            ), true );
                        }
                    }
                }
                else
                {
                    $ecommerceProductId = "-1";
                }

                // set the product for re-caching it's been changed
                MessageQueue::create()
                            ->queueMessage( self::PRODUCT_CACHE_QUEUE_NAME, $productId, true );

                // add the lookup to the cache for a day
                $this->cacheSet( $cacheKey, $ecommerceProductId, self::$_CACHE_TIME_ONE_WEEK );
            }
        }
    }

    /**
     * Function computes and saves a product's similar products.
     *
     * @param int $productId
     * @param int $numberOfSimilarProducts
     *
     * @return array
     * @throws CDbException
     */
    public function updateProductSimilarProducts( $productId, $numberOfSimilarProducts = 12 )
    {
        $this->deltaTimer( "START updateProductSimilarProducts $productId" );

        /** @var CDbCommand[] $commands */
        $commands = [];

        if( $similarProducts = $this->computeProductSimilarProducts( $productId, $numberOfSimilarProducts ) )
        {
            $similarProductIdBindings = [];
            $productSimilarityBindings = [];
            $bindingLocations = [];
            $counter = 0;
            foreach( $similarProducts as $similarProductId => $similarity )
            {
                $idBinding = ":similarId" . ++$counter;
                $similarityBinding = ":similarity$counter";
                $similarProductIdBindings[$idBinding] = $similarProductId;
                $productSimilarityBindings[$similarityBinding] = $similarity;
                $bindingLocations[] = "(:productId,$idBinding,$similarityBinding)";
            }
            $insertSQL = "INSERT INTO product_similar_product (product_id, similar_product_id, similarity_score) VALUES " . implode( ',', $bindingLocations ) . " ON DUPLICATE KEY UPDATE similarity_score = VALUES(similarity_score)";
            $commands['insert'] = $this->_db->createCommand( $insertSQL )
                                            ->bindValue( ':productId', $productId )
                                            ->bindValues( $similarProductIdBindings )
                                            ->bindValues( $productSimilarityBindings );

            $deleteSQL = "DELETE FROM product_similar_product WHERE product_id = :productId AND similar_product_id NOT IN (" . implode( ',', array_keys( $similarProductIdBindings ) ) . ");";
            $commands['delete'] = $this->_db->createCommand( $deleteSQL )
                                            ->bindValue( ':productId', $productId )
                                            ->bindValues( $similarProductIdBindings );

        }
        else
        {
            $deleteSQL = "DELETE FROM product_similar_product WHERE product_id = :productId;";
            $commands['delete'] = $this->_db->createCommand( $deleteSQL )
                                            ->bindValue( ':productId', $productId );

            $similarProducts = [];
        }

        $this->deltaTimer( "start transaction" );
        $transaction = $this->_db->beginTransaction();

        foreach( $commands as $commandType => $command )
        {
            $tries = 0;
            do
            {
                $retry = false;
                $tries += 1;
                try
                {
                    $result = $command->execute();

                    switch( $commandType )
                    {
                        case 'insert':
                            $this->deltaTimer( "inserted/updated $result rows" );
                            break;

                        case 'delete':
                            $this->deltaTimer( "deleted $result rows" );
                            break;
                    }
                }
                catch( CDbException $exception )
                {
                    if( $tries === 5 )
                    {
                        $transaction->rollback();
                        $this->deltaTimer( "failed transaction rolled back" );

                        throw new RuntimeException( "5th try failed", 0, $exception );
                    }
                    else
                    {
                        $retry = true;
                        usleep( rand( 100000, 500000 ) );
                    }
                }
            }
            while( $retry );

        }

        if( $this->_debug < 2 )
        {
            $transaction->commit();
            $this->deltaTimer( "successful transaction committed" );
        }
        else
        {
            $transaction->rollback();
            $this->deltaTimer( "successful transaction rolled back due to debug" );
        }

        $this->deltaTimer( "END updateProductSimilarProducts\n" );

        return $similarProducts;
    }

    /*
     * Function that computes the most similar products to the product given to it.
     * Returns an array of [product_id]=>similarity_score
     *
     * @param int $productId
     * @param int $numberOfSimilarProducts
     *
     * @return array
     *
    public function computeProductSimilarProducts( $productId, $numberOfSimilarProducts = 12 )
    {
        $this->deltaTimer( "START computeProductSimilarProducts $productId" );
        $rootCategoryId = Product::model()
                                 ->getRootCategoryId( $productId );
        switch( $rootCategoryId )
        {
            case self::musicRootId():
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name LIKE 'Music::Genres::%::%::%' AND category_name NOT LIKE '%::_ - _' AND category_name NOT LIKE '%::Decades';";
                $categoryOverlapScoreSQLSnippet = "SUM(SQRT(c1.relevance*c2.relevance/10000))";
                break;

            case self::moviesRootId():
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name NOT LIKE '%::Top Rated%' AND category_name NOT LIKE '%::Most Popular%'";
                $categoryOverlapScoreSQLSnippet = "COUNT(1)";
                break;

            case self::gamesRootId():
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name NOT LIKE '%::New Releases%' AND category_name NOT LIKE '%::Most Popular%'";
                $categoryOverlapScoreSQLSnippet = "COUNT(1)";
                break;

            default:
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId;";
                $categoryOverlapScoreSQLSnippet = "COUNT(1)";
        }

        if( $categoryIds = $this->_db->createCommand( $categoriesSQLQuery )
                                     ->queryColumn( [':productId' => $productId] )
        )
        {

            $this->deltaTimer( "found " . count( $categoryIds ) . " comparison categories for product" );

            $categoryIdBindings = [];
            $counter = 0;

            foreach( $categoryIds as $categoryId )
            {
                $categoryIdBindings[':catId' . $counter++] = $categoryId;
            }

            $productRelatedProductsSQL = "SELECT pt2.product_id,
SUM( SQRT( pt1.term_count/SQRT(ps1.product_term_total_count) * pt2.term_count/SQRT(ps2.product_term_total_count)/SQRT(ts.term_total_count))+0.005)*(3+c.category_overlap)*100 AS similarity_score
FROM product_term pt1 JOIN product_terms_summary ps1 ON pt1.product_id = ps1.product_id JOIN term_summary ts ON pt1.term_id=ts.term_id
JOIN product_term pt2 ON pt1.term_id=pt2.term_id JOIN product p ON p.product_id=pt2.product_id JOIN product_terms_summary ps2 ON pt2.product_id=ps2.product_id
JOIN (
SELECT c2.product_id, $categoryOverlapScoreSQLSnippet AS category_overlap
FROM category_product c1 JOIN category_product c2 ON c1.category_id=c2.category_id
WHERE c1.product_id = :productId AND c2.product_id != :productId
AND c1.category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ")
GROUP BY c2.product_id
) c ON pt2.product_id=c.product_id
WHERE pt1.product_id = :productId
AND p.product_root_category_id = :rootCategoryId AND p.display = 1 AND p.archived = 0 AND p.is_subproduct = 0
GROUP BY pt2.product_id ORDER BY similarity_score DESC LIMIT :limit;";

            $productRelatedProductsCommand = $this->_db->createCommand( $productRelatedProductsSQL )
                                                       ->bindValue( ':limit', $numberOfSimilarProducts )
                                                       ->bindValue( ':productId', $productId )
                                                       ->bindValue( ':rootCategoryId', $rootCategoryId )
                                                       ->bindValues( $categoryIdBindings );
        }
        else
        {

            $this->deltaTimer( "did not find any comparison categories for product" );

            $productRelatedProductsSQL = "SELECT pt2.product_id,
SUM( SQRT( pt1.term_count/SQRT(ps1.product_term_total_count) * pt2.term_count/SQRT(ps2.product_term_total_count)/SQRT(ts.term_total_count))+0.005)*100 AS similarity_score
FROM product_term pt1 JOIN product_terms_summary ps1 ON pt1.product_id = ps1.product_id JOIN term_summary ts ON pt1.term_id=ts.term_id
JOIN product_term pt2 ON pt1.term_id=pt2.term_id JOIN product p ON p.product_id=pt2.product_id JOIN product_terms_summary ps2 ON pt2.product_id=ps2.product_id
WHERE pt1.product_id = :productId
AND p.product_root_category_id = :rootCategoryId AND p.display = 1 AND p.archived = 0 AND p.is_subproduct = 0
GROUP BY pt2.product_id ORDER BY similarity_score DESC LIMIT :limit;";

            $productRelatedProductsCommand = $this->_db->createCommand( $productRelatedProductsSQL )
                                                       ->bindValue( ':limit', $numberOfSimilarProducts )
                                                       ->bindValue( ':productId', $productId )
                                                       ->bindValue( ':rootCategoryId', $rootCategoryId );

        }

        $productRelatedProductsResult = $productRelatedProductsCommand->queryAll();

        $this->deltaTimer( "END computeProductSimilarProducts with " . count( $productRelatedProductsResult ) . " similar products\n" );

        return array_column( $productRelatedProductsResult, 'similarity_score', 'product_id' );
    }

    /*
     * Function that computes the most similar products to the product given to it.
     * Returns an array of [product_id]=>similarity_score
     *
     * @param int $productId
     * @param int $numberOfSimilarProducts
     * @param int $intermediateProductCount
     *
     * @return array
     *
    public function computeProductSimilarProducts( $productId, $numberOfSimilarProducts = 12, $intermediateProductCount = 500 )
    {

        $this->deltaTimer( "START computeProductSimilarProducts $productId" );

        // Get term ids for the input product
        $termsSQL = "SELECT term_id FROM product_term WHERE product_id = :productId;";

        $termIds = $this->_db->createCommand( $termsSQL )
                             ->queryColumn( [':productId' => $productId] );
        $this->deltaTimer( "found " . count( $termIds ) . " comparison terms for product" );

        $termIdBindings = [];
        $termsCounter = 0;
        foreach( $termIds as $termId )
        {
            $termIdBindings[':termId' . $termsCounter++] = $termId;
        }

        // Get category ids for the input product
        $rootCategoryId = Product::model()
                                 ->getRootCategoryId( $productId );
        switch( $rootCategoryId )
        {
            case self::musicRootId():
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name LIKE 'Music::Genres::%::%::%' AND category_name NOT LIKE '%::_ - _' AND category_name NOT LIKE '%::Decades';";
                break;

            case self::moviesRootId():
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name NOT LIKE '%::Top Rated%' AND category_name NOT LIKE '%::Most Popular%'";
                break;

            case self::gamesRootId():
                $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name NOT LIKE '%::New Releases%' AND category_name NOT LIKE '%::Most Popular%'";
                break;

            default:
                $categoriesSQLQuery = "SELECT category_id FROM category_product WHERE product_id = :productId;";
        }

        if( $categoryIds = $this->_db->createCommand( $categoriesSQLQuery )
                                     ->queryColumn( [':productId' => $productId] )
        )
        {
            $this->deltaTimer( "found " . count( $categoryIds ) . " comparison categories for product" );

            $categoryIdBindings = [];
            $categoriesCounter = 0;
            foreach( $categoryIds as $categoryId )
            {
                $categoryIdBindings[':catId' . $categoriesCounter++] = $categoryId;
            }

            // Get the products that share the greatest number of terms and categories with the input product
            $closeProductsSQL = "SELECT
  t.product_id,
  t.term_num * c.category_num AS connections
FROM
  (
    SELECT
      product_id,
      COUNT(1) AS term_num
    FROM
      product_term
    WHERE
      term_id IN (" . implode( ',', array_keys( $termIdBindings ) ) . ")
      AND
      product_id != :productId
    GROUP BY
      product_id
  ) t
  JOIN
  (
    SELECT
      product_id,
      COUNT(1) AS category_num
    FROM
      category_product
    WHERE
      category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ")
      AND
      product_id != :productId
    GROUP BY
      product_id

  ) c
  ON
    t.product_id = c.product_id
ORDER BY
  connections DESC
LIMIT
  :limit";

            $closeProductsCommand = $this->_db->createCommand( $closeProductsSQL )
                                              ->bindValue( ':limit', (int)$intermediateProductCount )
                                              ->bindValue( ':productId', $productId )
                                              ->bindValues( $termIdBindings )
                                              ->bindValues( $categoryIdBindings );
        }
        else
        {
            $this->deltaTimer( "did not find any comparison categories for product" );

            // Get the products that share the greatest number of terms with the input product
            $closeProductsSQL = "SELECT
      product_id,
      COUNT(1) AS connections
    FROM
      product_term
    WHERE
      term_id IN (" . implode( ',', array_keys( $termIdBindings ) ) . ")
      AND
      product_id != :productId
    GROUP BY
      product_id
ORDER BY
  connections DESC
LIMIT
  :limit";

            $closeProductsCommand = $this->_db->createCommand( $closeProductsSQL )
                                              ->bindValue( ':limit', (int)$intermediateProductCount )
                                              ->bindValue( ':productId', $productId );

        }

        if( $closeProductIds = $closeProductsCommand->queryColumn() )
        {

            $this->deltaTimer( "Found " . count( $closeProductIds ) . " close products" );

            // check that the found products are valid
            $closeProductIdBindings = [];
            $closeCounter = 0;
            foreach( $closeProductIds as $closeProductId )
            {
                $closeProductIdBindings[":productId" . ++$closeCounter] = $closeProductId;
            }

            // this seems like the kind of thing that is/should be cached somewhere
            if( $validProductIds = $this->_db->createCommand( "SELECT product_id FROM product WHERE display = 1 AND archived = 0 AND is_subproduct = 0 AND product_id IN (" . implode( ',', array_keys( $closeProductIdBindings ) ) . ");" )
                                             ->queryColumn( $closeProductIdBindings )
            )
            {

                $this->deltaTimer( "Found " . count( $validProductIds ) . " valid products" );

                $validProductIdBindings = [];
                $validCounter = 0;
                foreach( $validProductIds as $validProductId )
                {
                    $validProductIdBindings[":validProductId" . ++$validCounter] = $validProductId;
                }

                // get each valid product's categories score
                if( isset( $categoryIdBindings ) )
                {
                    if( $rootCategoryId != self::musicRootId() )
                    {
                        $categoriesScoreCommand = $this->_db->createCommand( "SELECT product_id, COUNT(1) AS score FROM category_product WHERE product_id IN (" . implode( ',', array_keys( $validProductIdBindings ) ) . ") AND category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ") GROUP BY product_id" )
                                                            ->bindValues( $validProductIdBindings + $categoryIdBindings );
                    }
                    else
                    {
                        $categoriesScoreCommand = $this->_db->createCommand( "SELECT c2.product_id, SUM(SQRT(c1.relevance*c2.relevance/10000)) AS score FROM category_product c1 JOIN category_product c2 ON c1.category_id = c2.category_id WHERE c2.product_id IN (" . implode( ',', array_keys( $validProductIdBindings ) ) . ") AND c1.category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ") AND c1.product_id = :productId GROUP BY c2.product_id" )
                                                            ->bindValues( [':productId' => $productId] + $validProductIdBindings + $categoryIdBindings );
                    }

                    $categoryScores = array_column( $categoriesScoreCommand->queryAll(), 'score', 'product_id' );
                }
                else
                {
                    $categoryScores = array_fill_keys( $validProductIds, 1 );
                }
                $this->deltaTimer( "Found " . count( $categoryScores ) . " category scores" );

                // get each valid product's terms score
                $termScoresCommand = $this->_db->createCommand( "SELECT
  pt2.product_id,
  SUM(
    SQRT(
      pt1.term_count
      /
      SQRT(
        ps1.product_term_total_count
      )
      *
      pt2.term_count
      /
      SQRT(
        ps2.product_term_total_count
      )
      /
      SQRT(
        ts.term_total_count
      )
    )
    +
    0.005
  )
    AS
      score
FROM
  product_term pt1
  JOIN
  product_terms_summary ps1
    ON
      pt1.product_id = ps1.product_id
  JOIN
  term_summary ts
    ON
      pt1.term_id=ts.term_id
  JOIN
  product_term pt2
    ON
      pt1.term_id=pt2.term_id
  JOIN
  product p
    ON
      p.product_id=pt2.product_id
  JOIN
  product_terms_summary ps2
    ON
      pt2.product_id=ps2.product_id
WHERE
  pt1.product_id = :productId
  AND
  pt2.product_id
    IN (" . implode( ',', array_keys( $validProductIdBindings ) ) . ")
GROUP BY
  pt2.product_id;" )
                                               ->bindValues( [':productId' => $productId] + $validProductIdBindings );
                $termScores = array_column( $termScoresCommand->queryAll(), 'score', 'product_id' );
                $this->deltaTimer( "Found " . count( $termScores ) . " term scores" );

                // calculate total scores
                $productTotalScores = [];

                foreach( $validProductIds as $validProductId )
                {
                    if( isset( $termScores[$validProductId] ) && isset( $categoryScores[$validProductId] ) )
                    {
                        $productTotalScores[$validProductId] = $termScores[$validProductId] * ( 3 + $categoryScores[$validProductId] ) * 100;
                    }
                }
                $this->deltaTimer( "Found " . count( $productTotalScores ) . " total scores" );

                arsort( $productTotalScores );

                // select total scores
                $productTotalScores = array_slice( $productTotalScores, 0, $numberOfSimilarProducts, true );

                $this->deltaTimer( "END computeProductSimilarProducts with " . count( $productTotalScores ) . " similar products\n" );

                return $productTotalScores;
            }
        }

        $this->deltaTimer( "END computeProductSimilarProducts with no similar products\n" );

        return [];
    }*/

    /**
     * Function that computes the most similar products to the product given to it.
     * Returns an array of [product_id]=>similarity_score
     *
     * @param int $productId
     * @param int $numberOfSimilarProducts
     * @param int $intermediateProductCount
     *
     * @return array
     */
    public function computeProductSimilarProducts( $productId, $numberOfSimilarProducts = 12, $intermediateProductCount = 500 )
    {

        $this->deltaTimer( "START computeProductSimilarProducts $productId" );

        // Get term ids for the input product
        $termsSQL = "SELECT term_id FROM product_term NATURAL JOIN term_summary WHERE product_id = :productId ORDER BY term_count / SQRT(term_total_count) DESC LIMIT 25;";

        if( $termIds = $this->_db->createCommand( $termsSQL )
                                 ->queryColumn( [':productId' => $productId] )
        )
        {
            $this->deltaTimer( "found " . count( $termIds ) . " comparison terms for product" );

            $termIdBindings = [];
            $termsCounter = 0;
            foreach( $termIds as $termId )
            {
                $termIdBindings[':termId' . $termsCounter++] = $termId;
            }

            // Get category ids for the input product
            $rootCategoryId = Product::model()
                                     ->getRootCategoryId( $productId );
            switch( $rootCategoryId )
            {
                case self::musicRootId():
                    $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name LIKE 'Music::Genres::%::%::%' AND category_name NOT LIKE '%::_ - _' AND category_name NOT LIKE '%::Decades';";
                    break;

                case self::moviesRootId():
                case self::tvShowsRootId():
                    $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name NOT LIKE '%::Top Rated%' AND category_name NOT LIKE '%::Most Popular%'";
                    break;

                case self::gamesRootId():
                    $categoriesSQLQuery = "SELECT category_id FROM category NATURAL JOIN category_product WHERE product_id = :productId
AND category_name NOT LIKE '%::New Releases%' AND category_name NOT LIKE '%::Most Popular%'";
                    break;

                default:
                    $categoriesSQLQuery = "SELECT category_id FROM category_product WHERE product_id = :productId;";
            }

            // Get the products that share the greatest number of terms with the input product
            $termCountSQL = "SELECT
  product_id,
  COUNT(1) AS term_count
FROM
  product_term
WHERE
  term_id IN (" . implode( ',', array_keys( $termIdBindings ) ) . ")
  AND
  product_id != :productId
GROUP BY
  product_id
ORDER BY
  term_count DESC
LIMIT
  :limit";

            $termCountCommand = $this->_db->createCommand( $termCountSQL )
                                          ->bindValue( ':limit', $intermediateProductCount * 2 )
                                          ->bindValue( ':productId', $productId )
                                          ->bindValues( $termIdBindings );

            if( $termCountsArray = array_column( $termCountCommand->queryAll(), 'term_count', 'product_id' ) )
            {
                $this->deltaTimer( "Found " . count( $termCountsArray ) . " products with matching terms" );

                if( $categoryIds = $this->_db->createCommand( $categoriesSQLQuery )
                                             ->queryColumn( [':productId' => $productId] )
                )
                {
                    $this->deltaTimer( "found " . count( $categoryIds ) . " comparison categories for product" );

                    $termCountProductIdBindings = [];
                    $termCountCounter = 0;
                    foreach( $termCountsArray as $termCountProductId => $termCount )
                    {
                        $termCountProductIdBindings[":productId" . ++$termCountCounter] = $termCountProductId;
                    }

                    $categoryIdBindings = [];
                    $categoriesCounter = 0;
                    foreach( $categoryIds as $categoryId )
                    {
                        $categoryIdBindings[':catId' . $categoriesCounter++] = $categoryId;
                    }

                    $categoryCountSQL = "SELECT
  product_id,
  COUNT(1) AS category_count
FROM
  category_product
WHERE
  category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ")
  AND
  product_id IN (" . implode( ',', array_keys( $termCountProductIdBindings ) ) . ")
GROUP BY
  product_id";

                    $categoryCountCommand = $this->_db->createCommand( $categoryCountSQL )
                                                      ->bindValues( $termCountProductIdBindings )
                                                      ->bindValues( $categoryIdBindings );

                    $categoryCountArray = array_column( $categoryCountCommand->queryAll(), 'category_count', 'product_id' );
                    $this->deltaTimer( "Found " . count( $categoryCountArray ) . " products with matching categories" );

                    $combinedCountArray = [];

                    foreach( $categoryCountArray as $categoryCountProductId => $categoryCount )
                    {
                        $combinedCountArray[$categoryCountProductId] = $categoryCount * $termCountsArray[$categoryCountProductId];
                    }

                    arsort( $combinedCountArray );
                    $closeProductIds = array_slice( array_keys( $combinedCountArray ), 0, $intermediateProductCount );
                }
                else
                {
                    $closeProductIds = array_slice( array_keys( $termCountsArray ), 0, $intermediateProductCount );
                }

                if( $closeProductIds )
                {
                    // check that the found products are valid
                    $closeProductIdBindings = [];
                    $closeCounter = 0;
                    foreach( $closeProductIds as $termCountProductId )
                    {
                        $closeProductIdBindings[":productId" . ++$closeCounter] = $termCountProductId;
                    }

                    $isSubproduct = $this->_db->createCommand( "SELECT is_subproduct FROM product WHERE product_id = :productId;" )
                                              ->queryScalar( [':productId' => $productId] );

                    // todo: this seems like the kind of thing that is/should be cached somewhere
                    if( $validProductIds = $this->_db->createCommand( "SELECT product_id FROM product WHERE display = 1 AND archived = 0 AND is_subproduct = :isSubproduct AND product_id IN (" . implode( ',', array_keys( $closeProductIdBindings ) ) . ");" )
                                                     ->bindValues( $closeProductIdBindings )
                                                     ->bindValue( ':isSubproduct', ( $isSubproduct ? 1 : 0 ) )
                                                     ->queryColumn()
                    )
                    {

                        $this->deltaTimer( "Found " . count( $validProductIds ) . " valid products" );

                        $validProductIdBindings = [];
                        $validCounter = 0;
                        foreach( $validProductIds as $validProductId )
                        {
                            $validProductIdBindings[":validProductId" . ++$validCounter] = $validProductId;
                        }

                        // get each valid product's categories score
                        if( isset( $categoryIdBindings ) )
                        {
                            if( $rootCategoryId != self::musicRootId() )
                            {
                                $categoriesScoreCommand = $this->_db->createCommand( "SELECT product_id, COUNT(1) AS score FROM category_product WHERE product_id IN (" . implode( ',', array_keys( $validProductIdBindings ) ) . ") AND category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ") GROUP BY product_id" )
                                                                    ->bindValues( $validProductIdBindings + $categoryIdBindings );
                            }
                            else
                            {
                                $categoriesScoreCommand = $this->_db->createCommand( "SELECT c2.product_id, SUM(SQRT(c1.relevance*c2.relevance/10000)) AS score FROM category_product c1 JOIN category_product c2 ON c1.category_id = c2.category_id WHERE c2.product_id IN (" . implode( ',', array_keys( $validProductIdBindings ) ) . ") AND c1.category_id IN (" . implode( ',', array_keys( $categoryIdBindings ) ) . ") AND c1.product_id = :productId GROUP BY c2.product_id" )
                                                                    ->bindValues( [':productId' => $productId] + $validProductIdBindings + $categoryIdBindings );
                            }

                            $categoryScores = array_column( $categoriesScoreCommand->queryAll(), 'score', 'product_id' );
                        }
                        else
                        {
                            $categoryScores = array_fill_keys( $validProductIds, 1 );
                        }
                        $this->deltaTimer( "Found " . count( $categoryScores ) . " category scores" );

                        // get each valid product's terms score
                        $termScoresCommand = $this->_db->createCommand( "
SELECT
  pt2.product_id,
  SUM(
    SQRT(
      pt1.term_count
      /
      SQRT(
        ps1.product_term_total_count
      )
      *
      pt2.term_count
      /
      SQRT(
        ps2.product_term_total_count
      )
      /
      SQRT(
        ts.term_total_count
      )
    )
    +
    0.005
  )
    AS
      score
FROM
  product_term pt1
  JOIN
  product_terms_summary ps1
    ON
      pt1.product_id = ps1.product_id
  JOIN
  term_summary ts
    ON
      pt1.term_id=ts.term_id
  JOIN
  product_term pt2
    ON
      pt1.term_id=pt2.term_id
  JOIN
  product p
    ON
      p.product_id=pt2.product_id
  JOIN
  product_terms_summary ps2
    ON
      pt2.product_id=ps2.product_id
WHERE
  pt1.product_id = :productId
  AND
  pt2.product_id
    IN (" . implode( ',', array_keys( $validProductIdBindings ) ) . ")
GROUP BY
  pt2.product_id
  " )
                                                       ->bindValues( [':productId' => $productId] + $validProductIdBindings );
                        $termScores = array_column( $termScoresCommand->queryAll(), 'score', 'product_id' );
                        $this->deltaTimer( "Found " . count( $termScores ) . " term scores" );

                        // calculate total scores
                        $productTotalScores = [];

                        foreach( $validProductIds as $validProductId )
                        {
                            if( isset( $termScores[$validProductId] ) && isset( $categoryScores[$validProductId] ) )
                            {
                                $productTotalScores[$validProductId] = $termScores[$validProductId] * ( 3 + $categoryScores[$validProductId] ) * 100;
                            }
                        }
                        $this->deltaTimer( "Found " . count( $productTotalScores ) . " total scores" );

                        arsort( $productTotalScores );

                        // select total scores
                        $productTotalScores = array_slice( $productTotalScores, 0, $numberOfSimilarProducts, true );

                        $this->deltaTimer( "END computeProductSimilarProducts with " . count( $productTotalScores ) . " similar products\n" );

                        return $productTotalScores;
                    }
                }
            }
        }
        $this->deltaTimer( "END computeProductSimilarProducts with no similar products\n" );

        return [];
    }

    /**
     * Get products positively or negatively marked as of user interest
     *
     * @param $userId
     * @param $interested
     * @param $limitString
     *
     * @return array
     */
    public function getUserInterestProducts( $userId, $interested, $limitString )
    {
        // get sub products
        $productLookupSQL = "
				SELECT product.product_id
				FROM product
				JOIN product_image ON product.product_id = product_image.product_id
				JOIN user_interest ON product.product_id = user_interest.product_id
				WHERE user_interest.user_id = $userId
				AND user_interest.interested = $interested
                AND product.product_root_category_id IN (" . $this->getRootCategoriesCSV( 'GB' ) . ")
				AND product.display = 1
				GROUP BY product.product_name_md5, product.product_root_category_id
				ORDER BY user_interest.last_updated DESC
				LIMIT $limitString
				";

        // lookup and return the products
        $productSummary = $this->_db->createCommand( $productLookupSQL )
                                    ->queryAll();

        // get the required elements
        $requiredElements = array(
            'product_name',
            'root_category_id',
            'product_average_rating',
            'product_num_ratings',
            'product_num_reviews',
            'product_short_description',
            'release_date',
            'categories',
            'images',
        );

        $products = $this->consolidateProductLookup( $productSummary, $requiredElements, 0 );

        return $products;
    }

    /**
     * Get the set of User Interests (itchlist / scraplist)
     *
     * @param $item
     *
     * @return array
     */
    public function getUserInterests( $item )
    {
        // get sub products
        $userInterestsSQL = "
				SELECT product.product_id, user_interest.interested
				FROM product
				JOIN product_image ON product.product_id = product_image.product_id
				JOIN user_interest ON product.product_id = user_interest.product_id
				WHERE user_interest.user_id = $item
                AND product.product_root_category_id IN (" . $this->getRootCategoriesCSV( 'GB' ) . ")
				AND product.display = 1
				GROUP BY product.product_name_md5, product.product_root_category_id
				ORDER BY user_interest.last_updated DESC
				";

        // lookup and return the userInterest
        $userInterests = $this->_db->createCommand( $userInterestsSQL )
                                   ->queryAll();

        // return data
        return $userInterests;
    }

    /**
     * Get Ecommerce Providers by country and by category
     *
     * @param int    $categoryId
     * @param string $countryCode
     *
     * @return array
     */
    public function getEcommerceProviders( $categoryId, $countryCode = null )
    {
        $results = array();
        $categoryFilter = '';
        if( $categoryId )
        {
            $categoryFilter = "WHERE category_id = $categoryId ";
        }
        if( $countryCode )
        {
            if( $categoryId )
            {
                $categoryFilter .= "AND country_code = '$countryCode' ";
            }
            else
            {
                $categoryFilter = "WHERE country_code = '$countryCode' ";
            }
        }
        $ecommerceProviderSQL = "
SELECT
category_id,
  ecommerce_provider_type_id,
  ecommerce_provider_name,
  ecommerce_provider_logo_url,
  country_code
FROM
  ecommerce_provider NATURAL
  JOIN ecommerce_provider_category NATURAL
  JOIN ecommerce_provider_type
$categoryFilter
ORDER BY country_code, ecommerce_provider_type_id, ecommerce_provider_name
";

        // lookup and return the userInterest
        $ecommerceProviders = $this->_db->createCommand( $ecommerceProviderSQL )
                                        ->queryAll();

        foreach( $ecommerceProviders as $ecommerceProvider )
        {
            if( $ecommerceProvider['ecommerce_provider_type_id'] == self::ECOMMERCE_PROVIDER_TYPE_INDIVIDUAL_ITEM )
            {
                $ecommerceGroupLabel = "Buy it on";
            }
            else
            {
                switch( $categoryId )
                {
                    case self::moviesRootId():
                        $ecommerceGroupLabel = "Watch it on";
                        break;
                    case self::tvShowsRootId():
                        $ecommerceGroupLabel = "Watch it on";
                        break;
                    case self::gamesRootId():
                        $ecommerceGroupLabel = "Play it on";
                        break;
                    case self::musicRootId():
                        $ecommerceGroupLabel = "Listen to it on";
                        break;
                    case self::booksRootId():
                        $ecommerceGroupLabel = "Read it on";
                        break;
                    default:
                        $ecommerceGroupLabel = "Read it on";
                        break;

                }
            }
            if( $countryCode )
            {
                $results[$ecommerceGroupLabel][$ecommerceProvider['ecommerce_provider_name']] = array(
                    'ecommerce_product_provider_name'     => $ecommerceProvider['ecommerce_provider_name'],
                    'ecommerce_product_provider_logo_url' => $ecommerceProvider['ecommerce_provider_logo_url'],
                );
            }
            else
            {
                $results['ecommerce_providers'][$ecommerceProvider['country_code']][$ecommerceGroupLabel][$ecommerceProvider['ecommerce_provider_name']] = array(
                    'ecommerce_product_provider_name'     => $ecommerceProvider['ecommerce_provider_name'],
                    'ecommerce_product_provider_logo_url' => $ecommerceProvider['ecommerce_provider_logo_url'],
                );
            }
        }

        return $results;
    }
}