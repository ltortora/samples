<?php

/**
 * Created by Lucas Tortora
 * Date: 2016-06-14
 * Time: 04:21:57
 * @incomplete @todo test other methods other than ecommerce related ones
 */
class ProductServiceTest extends CDbTestCase
{

    /**
     * @var $productService ProductService
     */
    private $productService;

    public function setUp()
    {

        $this->fixtureManager = $this->getFixtureManager();
        $this->productService = new ProductService();
    }

    /**
     * @covers ProductService::getProductEcommerceLinks()
     */
    public function testGetProductEcommerceLinks()
    {

        $this->fixtureManager->load( array('ecommerce_provider_product'                => 'EcommerceProviderProduct',
                                           'ecommerce_provider_product_admin_override' => 'EcommerceProviderProductAdminOverride'
        ) );

        $adminRecordId = $this->fixtureManager->getRecord( 'ecommerce_provider_product_admin_override', 'override' )
                                              ->getAttribute( 'product_id' );
        $adminRecordExpectedData = EcommerceProviderProductAdminOverride::model()
                                                                        ->with( 'ecommerceProvider' )
                                                                        ->together()
                                                                        ->findByAttributes( array('product_id' => $adminRecordId) );
        //assert the adminUser links is used is set on both
        $links = $this->productService->getProductEcommerceLinks( $adminRecordId, 1072 );
        $expectedLinkData = array_merge( $adminRecordExpectedData->getAttributes(),

            $adminRecordExpectedData->ecommerceProvider->getAttributes() );
        $ecommerceProviderName = EcommerceProvider::model()
                                                  ->findByPk( $expectedLinkData['ecommerce_provider_id'] )
                                                  ->getAttribute( 'ecommerce_provider_name' );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_name"], $expectedLinkData['ecommerce_product_provider_name'] );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_logo_url"], $expectedLinkData['ecommerce_product_provider_logo_url'] );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_name"], $expectedLinkData['ecommerce_product_name'] );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_link"], $expectedLinkData['ecommerce_product_link'] );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_image_url"], $expectedLinkData['ecommerce_product_image_url'] );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_price"], $expectedLinkData['ecommerce_product_price'] );
        $this->assertEquals( $links[$expectedLinkData['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_currency_code"], $expectedLinkData['ecommerce_product_currency_code'] );

        //assert empty  on exclude
        $links = $this->productService->getProductEcommerceLinks( $this->fixtureManager->getRecord( 'ecommerce_provider_product', 'exclude' )
                                                                                       ->getAttribute( 'product_id' ), 1072 );
        $this->assertEmpty( $links );

        //assert both records are returned if not overwritten or excluded
        $links = $this->productService->getProductEcommerceLinks( $this->fixtureManager->getRecord( 'ecommerce_provider_product', 'merge' )
                                                                                       ->getAttribute( 'product_id' ), 1072 );
        $expectedData = array();

        $adminAttributesProductId = $this->fixtureManager->getRecord( 'ecommerce_provider_product_admin_override', 'merge' )
                                                         ->getAttribute( 'product_id' );

        $adminRecordExpectedData = EcommerceProviderProductAdminOverride::model()
                                                                        ->with( 'ecommerceProvider' )
                                                                        ->together()
                                                                        ->findByAttributes( array('product_id' => $adminAttributesProductId) );
        $adminAttributes = array_merge( $adminRecordExpectedData->getAttributes(), $adminRecordExpectedData->ecommerceProvider->getAttributes() );
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_name"] = $adminAttributes['ecommerce_product_provider_name'];
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_link"] = $adminAttributes['ecommerce_product_link'];
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_image_url"] = $adminAttributes['ecommerce_product_image_url'];
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_price"] = $adminAttributes['ecommerce_product_price'];
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_currency_code"] = $adminAttributes['ecommerce_product_currency_code'];
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_logo_url"] = $adminAttributes['ecommerce_product_provider_logo_url'];
        $expectedData[$adminAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_logo_url"] = $adminAttributes['ecommerce_product_provider_logo_url'];

        $importedAttributesProductId = $this->fixtureManager->getRecord( 'ecommerce_provider_product', 'merge' )
                                                            ->getAttribute( 'product_id' );
        $importedRecordExpectedData = EcommerceProviderProductAdminOverride::model()
                                                                           ->with( 'ecommerceProvider' )
                                                                           ->together()
                                                                           ->findByAttributes( array('product_id' => $importedAttributesProductId) );
        $importedAttributes = array_merge( $importedRecordExpectedData->getAttributes(), $importedRecordExpectedData->ecommerceProvider->getAttributes() );
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName] = $importedAttributes['ecommerce_product_provider_name'];
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_link"] = $importedAttributes['ecommerce_product_link'];
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_image_url"] = $importedAttributes['ecommerce_product_image_url'];
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_price"] = $importedAttributes['ecommerce_product_price'];
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_currency_code"] = $importedAttributes['ecommerce_product_currency_code'];
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_logo_url"] = $importedAttributes['ecommerce_product_provider_logo_url'];
        $expectedData[$importedAttributes['country_code']]["Buy it on"][$ecommerceProviderName]["ecommerce_product_provider_logo_url"] = $importedAttributes['ecommerce_product_provider_logo_url'];

        $linkData = $this->productService->getProductEcommerceLinks( $this->fixtureManager->getRecord( 'ecommerce_provider_product', 'merge' )
                                                                                          ->getAttribute( 'product_id' ), 1072 );
        $this->assertEquals( sort( $expectedData ), sort( $linkData['ecommerce_product_links'] ) );


    }

}