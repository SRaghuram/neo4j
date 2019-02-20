MATCH (message:Message {id:{1}})
RETURN
 message.creationDate AS messageCreationDate,
 coalesce(message.imageFile,message.content) AS messageContent
