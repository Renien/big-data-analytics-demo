SELECT * FROM fb_feed
WHERE friend_count > 100

SELECT * FROM fb_feed
WHERE friend_count > 100 AND gender == 'male'

SELECT COUNT(*) FROM fb_feed
WHERE friendships_initiated = 1 AND gender == 'male'
Ans: 1525

SELECT COUNT(*) FROM fb_feed
WHERE friendships_initiated = 1 AND gender == 'female'
Ans: 685

SELECT *
FROM fb_feed
WHERE fb_feed.likes_received IN
(SELECT a.* FROM
(SELECT MAX(likes_received) AS likes_received
FROM fb_feed) a);
