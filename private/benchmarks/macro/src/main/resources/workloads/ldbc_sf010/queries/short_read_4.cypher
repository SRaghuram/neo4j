MATCH (message:Message {id:{Message}})
RETURN
 message.creationDate AS messageCreationDate,
 coalesce(message.imageFile,message.content) AS messageContent
