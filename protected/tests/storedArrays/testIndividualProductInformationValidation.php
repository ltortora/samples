<?php
/**
 * Created by PhpStorm.
 * User: Lucas
 * Date: 23/06/2016
 * Time: 09:42
 */
//product 1 should discard one product, all but 1 product attributes
//product 2 should return empty dpp except dp25
//product 3 shoudl return all dps as echonest check is false
return array(
    'product_1' => array(
        'product'               => array(
            array(
                "product_id"          => 1,
                "product_name"        => "Test",
                "product_sort_name"   => "Test",
                "product_description" => "Test Band Members",
                "product_link"        => "http://itunes.apple.com/artist/pink-floyd/id487143?uo=5",
            ),
            array(
                "product_id"          => 2,
                "product_name"        => "Test",
                "product_sort_name"   => "Test",
                "product_description" => "Test Band Members",
                "product_link"        => "http://itunes.apple.com/artist/pink-floyd/id487143?uo=5",
            )
        ),
        'product_attribute'     => array(
            array(
                "id"                        => 33779038,
                "product_id"                => 1,
                "product_attribute_type_id" => 57,
                "product_attribute_value"   => "test"
            ),
            array(
                "id"                        => 33779038,
                "product_id"                => 2,
                "product_attribute_type_id" => 57,
                "product_attribute_value"   => "test"
            ),
            array(
                "id"                        => 33779038,
                "product_id"                => 3,
                "product_attribute_type_id" => 57,
                "product_attribute_value"   => "test"
            ),
        ),
        'data_provider_product' => array(
            array(
                'product_id'               => 1,
                'data_provider_id'         => 25,
                'data_provider_product_id' => 'aTEST',
                'product_attributes'       => json_encode( array('members' => array('John', 'James', 'Jill')) )
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 8,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 27,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 28,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 29,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 3,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 3,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 3,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 3,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 3,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
            array(
                'product_id'               => 3,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'John james and jill'
            ),
        ),
    ),
    'product_2' => array(
        'product'               => array(
            array(
                "product_id"          => 1,
                "product_name"        => "Test",
                "product_sort_name"   => "Test",
                "product_description" => "Test Band Members",
                "product_link"        => "http://itunes.apple.com/artist/pink-floyd/id487143?uo=5",
            )
        ),
        'product_attribute'     => array(
            array(
                "id"                        => 33779038,
                "product_id"                => 1,
                "product_attribute_type_id" => 57,
                "product_attribute_value"   => "test"
            )
        ),
        'data_provider_product' => array(
            array(
                'product_id'               => 1,
                'data_provider_id'         => 25,
                'data_provider_product_id' => 'aTEST',
                'product_attributes'       => json_encode( array('members' => array('John', 'James', 'Jill')) )
            ),
            array(
                'product_id'               => 1,
                'data_provider_id'         => 8,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'fail',
                'product_attributes'       => 'fail'
            ),
            array(
                'product_id'               => 1,
                'data_provider_id'         => 27,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'fail',
                'product_attributes'       => 'fail'
            ),
            array(
                'product_id'               => 1,
                'data_provider_id'         => 28,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'fail',
                'product_attributes'       => 'fail'
            ),
            array(
                'product_id'               => 1,
                'data_provider_id'         => 29,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'fail',
                'product_attributes'       => 'fail'
            ),
            array(
                'product_id'               => 1,
                'data_provider_id'         => 35,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'fail',
                'product_attributes'       => 'fail'
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'fail',
                'product_attributes'       => 'fail'
            ),
        ),
    ),
    'product_3' => array(
        'product'               => array(
            array(
                "product_id"          => 1,
                "product_name"        => "Test",
                "product_sort_name"   => "Test",
                "product_description" => "Test Band Members",
                "product_link"        => "http://itunes.apple.com/artist/pink-floyd/id487143?uo=5",
            )
        ),
        'product_attribute'     => array(
            array(
                "id"                        => 33779038,
                "product_id"                => 1,
                "product_attribute_type_id" => 57,
                "product_attribute_value"   => "test"
            )
        ),
        'data_provider_product' => array(
            array(
                'product_id'               => 1,
                'data_provider_id'         => 25,
                'data_provider_product_id' => 'aTEST',
                'product_attributes'       => json_encode( array('members' => array('John', 'James', 'Jill')) )
            ),
            array(
                'product_id'               => 2,
                'data_provider_id'         => 3,
                'data_provider_product_id' => 'aTEST',
                'product_description'      => 'John james and jill',
                'product_attributes'       => 'no pass',

            ),
        ),
    ),
);