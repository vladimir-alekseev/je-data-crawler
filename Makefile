.PHONY: docker-images
docker-images:
	docker buildx build --platform linux/amd64,linux/arm/v7 -t vladimiralekseev/je-data-crawler --push . 