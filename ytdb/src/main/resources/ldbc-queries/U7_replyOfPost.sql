CREATE EDGE REPLY_OF FROM (SELECT FROM Comment WHERE id = :commentId)
  TO (SELECT FROM Post WHERE id = :replyToPostId)