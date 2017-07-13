<?php
/**
 * Created by PhpStorm.
 * User: Lucas
 * Date: 26/05/2016
 * Time: 08:16
 */

$rows = array(
    array(
        "user_id"          => 2620973,
        "product_id"       => 13548515,
        "root_category_id" => 24101,
        "rating"           => 4,
        "last_updated"     => "2014-04-26 19:03:36",
        "row_updated"      => "2015-05-15 14:07:06"
    ),
    array(
        "user_id"          => 1642672,
        "product_id"       => 13548515,
        "root_category_id" => 24101,
        "rating"           => 5,
        "last_updated"     => "2014-04-17 19:37:39",
        "row_updated"      => "2015-05-15 14:07:06"
    ),
    array(
        "user_id"          => 2661689,
        "product_id"       => 13548515,
        "root_category_id" => 24101,
        "rating"           => 5,
        "last_updated"     => "2014-04-17 19:37:39",
        "row_updated"      => "2015-05-15 14:07:06"
    ),
    array(
        "user_id"          => 2620973,
        "product_id"       => 13440313,
        "root_category_id" => 24101,
        "rating"           => 4,
        "last_updated"     => "2014-04-26 19:03:36",
        "row_updated"      => "2015-05-15 14:07:06"
    ),
    array(
        "user_id"          => 1642672,
        "product_id"       => 13440313,
        "root_category_id" => 24101,
        "rating"           => 5,
        "last_updated"     => "2014-04-17 19:37:39",
        "row_updated"      => "2015-05-15 14:07:06"
    ),
    array(
        "user_id"          => 2661689,
        "product_id"       => 13440313,
        "root_category_id" => 24101,
        "rating"           => 5,
        "last_updated"     => "2014-04-17 19:37:39",
        "row_updated"      => "2015-05-15 14:07:06"
    )
);

//add 50 rows of matching user_+rating for 2 products to test duplicate detection by ratings
for( $i = 0; $i < 50; $i++ )
{

    $rows[] = array(
        "user_id"          => 1274385 + $i,
        "product_id"       => 364299,
        "root_category_id" => 24101,
        "rating"           => 5,
        "last_updated"     => "2012-12-05 00:00:00",
        "row_updated"      => "2015-05-15 14:07:06",
    );
    $rows[] = array(
        "user_id"          => 1274385 + $i,
        "product_id"       => 546897,
        "root_category_id" => 24101,
        "rating"           => 5,
        "last_updated"     => "2013-11-28 03:02:12",
        "row_updated"      => "2015-05-15 14:07:06"
    );
}
return $rows;