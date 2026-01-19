package rdb

import (
	"context"
)

//	EXPIRE key seconds, 给指定key设置过期时间
//
// return int, 1 成功， 0 失败
func (b builder) Expire(ctx context.Context, cmd RdCmd, args map[string]any, includeArgs ...any) *CommandBuilder {
	return b(ctx, cmd, EXPIRE, args, includeArgs...)
}

//	TTL key  查询指定key的过期时间
//
// return int, >=0 有过期时间， -1 存在且永久有效， -2 不存在或过期
func (b builder) Ttl(ctx context.Context, cmd RdCmd, args map[string]any, includeArgs ...any) *CommandBuilder {
	return b(ctx, cmd, TTL, args, includeArgs...)
}
