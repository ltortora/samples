<?php
/**
 * Created by PhpStorm.
 * User=> Lucas
 * Date=> 17/05/2016
 * Time=> 14=>39
 */
$rows = array(
    array(
        "review_id"    => 7271276,
        "user_id"      => 2620973,
        "product_id"   => 13548515,
        "review_note"  => "Absolutely loved this book, and didn\'t want it to end. I never knew about the battle going on beneath no mans land. Felt SF went some way to capture the horror of the unthinkable.",
        "last_updated" => "2014-02-15 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2014-02-15 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    ),
    array(
        "review_id"    => 7271277,
        "user_id"      => 1642672,
        "product_id"   => 13548515,
        "review_note"  => "This book was ok.  I wasn\'t keen on the tv adaptation so I thought I would read the book.  It wasn\'t really to my taste",
        "last_updated" => "2014-02-12 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2014-02-12 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    ),
    array(
        "review_id"    => 7271278,
        "user_id"      => 2661689,
        "product_id"   => 13548515,
        "review_note"  => "Well written novel where the reader can feel the tension and empathize with the people involved. The atmosphere of the beginning of the twentieth century is well described.",
        "last_updated" => "2014-02-09 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2014-02-09 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    ),
    array(
        "review_id"    => 7271279,
        "user_id"      => 2620973,
        "product_id"   => 13440313,
        "review_note"  => "Absolutely loved this book, and didn\'t want it to end. I never knew about the battle going on beneath no mans land. Felt SF went some way to capture the horror of the unthinkable.",
        "last_updated" => "2014-02-15 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2014-02-15 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    ),
    array(
        "review_id"    => 7271280,
        "user_id"      => 1642672,
        "product_id"   => 13440313,
        "review_note"  => "This book was ok.  I wasn\'t keen on the tv adaptation so I thought I would read the book.  It wasn\'t really to my taste",
        "last_updated" => "2014-02-12 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2014-02-12 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    ),
    array(
        "review_id"    => 7271281,
        "user_id"      => 2661689,
        "product_id"   => 13440313,
        "review_note"  => "Well written novel where the reader can feel the tension and empathize with the people involved. The atmosphere of the beginning of the twentieth century is well described.",
        "last_updated" => "2014-02-09 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2014-02-09 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    ),

);

//add 50 rows of matching user_+rating for 2 products to test duplicate detection by ratings
for( $i = 0; $i < 50; $i++ )
{
    $rows[] = array(
        "review_id"    => null,
        "user_id"      => 1274385 + $i,
        "product_id"   => 364299,
        "review_note"  => "Bought this to put on the table at my daughters mad hatter tea party (21st) went down a storm with old and young",
        "last_updated" => "2012-12-05 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2012-12-05 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53",
    );
    $rows[] = array(
        "review_id"    => null,
        "user_id"      => 1274385 + $i,
        "product_id"   => 546897,
        "review_note"  => "Bought this to put on the table at my daughters mad hatter tea party (21st) went down a storm with old and young",
        "last_updated" => "2012-12-05 00:00:00",
        "num_comments" => 0,
        "num_likes"    => 0,
        "num_dislikes" => 0,
        "review_date"  => "2012-12-05 00:00:00",
        "review_link"  => null,
        "row_updated"  => "2015-05-15 10:46:53"
    );
}
return $rows;