package rdb

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
)

// CommandBuilder 命令构建器，支持链式调用
// 同时实现 redis.Cmder 接口，以便可以直接作为 redis.Cmder 使用
type CommandBuilder struct {
	client      *RedisClient
	pipeliner   redis.Pipeliner // 如果设置，表示在 Pipeline 中
	ctx         context.Context
	cmd         RdCmd
	cmdName     Command
	args        map[string]any
	includeArgs []any
	cmder       redis.Cmder // 缓存的 cmder，用于实现 redis.Cmder 接口
}

// 实现 redis.Cmder 接口，以便 CommandBuilder 可以直接作为 redis.Cmder 使用
func (cb *CommandBuilder) Name() string {
	if cb.cmder != nil {
		return cb.cmder.Name()
	}
	return string(cb.cmdName)
}

func (cb *CommandBuilder) Args() []interface{} {
	if cb.cmder != nil {
		return cb.cmder.Args()
	}
	cmdList, _, _ := Build(cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
	return cmdList
}

func (cb *CommandBuilder) SetErr(err error) {
	if cb.cmder != nil {
		if setter, ok := cb.cmder.(interface{ SetErr(error) }); ok {
			setter.SetErr(err)
		}
	}
}

func (cb *CommandBuilder) Err() error {
	// 如果还未执行，使用默认的 *redis.Cmd 执行
	if cb.cmder == nil {
		cmdList, key, subCmd := Build(cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cmder := redis.NewCmd(cb.ctx, cmdList...)

		if cb.pipeliner != nil {
			_ = cb.pipeliner.Process(cb.ctx, cmder)
			if subCmd.Exp != nil {
				exp := subCmd.Exp()
				cb.pipeliner.Expire(cb.ctx, key, exp)
			}
			cb.cmder = cmder
		} else {
			processErr := cb.client.Client.Process(cb.ctx, cmder)
			cmdErr := cmder.Err()
			if processErr != nil {
				cmdErr = processErr
			}
			if !subCmd.ReturnNilError && errors.Is(cmdErr, redis.Nil) {
				cmdErr = nil
			}
			if cmdErr != nil {
				cmder.SetErr(cmdErr)
			}
			if subCmd.Exp != nil {
				exp := subCmd.Exp()
				expireCmd := cb.client.Client.Expire(cb.ctx, key, exp)
				if expireCmd.Err() != nil {
					// 记录错误但不影响主命令
				}
			}
			cb.cmder = cmder
		}
	}
	if cb.cmder != nil {
		return cb.cmder.Err()
	}
	return nil
}

func (cb *CommandBuilder) Val() interface{} {
	// 如果还未执行，使用默认的 *redis.Cmd 执行
	if cb.cmder == nil {
		cmdList, key, subCmd := Build(cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cmder := redis.NewCmd(cb.ctx, cmdList...)

		if cb.pipeliner != nil {
			_ = cb.pipeliner.Process(cb.ctx, cmder)
			if subCmd.Exp != nil {
				exp := subCmd.Exp()
				cb.pipeliner.Expire(cb.ctx, key, exp)
			}
			cb.cmder = cmder
		} else {
			processErr := cb.client.Client.Process(cb.ctx, cmder)
			cmdErr := cmder.Err()
			if processErr != nil {
				cmdErr = processErr
			}
			if !subCmd.ReturnNilError && errors.Is(cmdErr, redis.Nil) {
				cmdErr = nil
			}
			if cmdErr != nil {
				cmder.SetErr(cmdErr)
			}
			if subCmd.Exp != nil {
				exp := subCmd.Exp()
				expireCmd := cb.client.Client.Expire(cb.ctx, key, exp)
				if expireCmd.Err() != nil {
					// 记录错误但不影响主命令
				}
			}
			cb.cmder = cmder
		}
	}
	if cb.cmder != nil {
		if valProvider, ok := cb.cmder.(interface{ Val() interface{} }); ok {
			return valProvider.Val()
		}
	}
	return nil
}

// NewCommandBuilder 创建命令构建器
func NewCommandBuilder(client *RedisClient, ctx context.Context, cmd RdCmd, cmdName Command, args map[string]any, includeArgs ...any) *CommandBuilder {
	return &CommandBuilder{
		client:      client,
		ctx:         ctx,
		cmd:         cmd,
		cmdName:     cmdName,
		args:        args,
		includeArgs: includeArgs,
	}
}

// NewPipelineCommandBuilder 创建 Pipeline 命令构建器
func NewPipelineCommandBuilder(pipeliner redis.Pipeliner, ctx context.Context, cmd RdCmd, cmdName Command, args map[string]any, includeArgs ...any) *CommandBuilder {
	return &CommandBuilder{
		pipeliner:   pipeliner,
		ctx:         ctx,
		cmd:         cmd,
		cmdName:     cmdName,
		args:        args,
		includeArgs: includeArgs,
	}
}

// BuildCmd 构建 Redis 命令但不执行，返回构建好的 redis.Cmder
// 这个方法可以让你构建命令，然后自己决定如何执行
func (rdm RedisClient) BuildCmd(ctx context.Context, cmd RdCmd, cmdName Command, args map[string]any, includeArgs ...any) redis.Cmder {
	cmdList, _, _ := Build(ctx, cmd, cmdName, args, includeArgs...)
	return redis.NewCmd(ctx, cmdList...)
}

// ExecuteCmd 执行命令并返回具体的类型
// 这是一个泛型方法，根据泛型类型 T 自动创建对应的 redis.Cmder
// 错误通过返回的 Cmder 的 Err() 方法获取
// 使用示例：
//
//	cmd := client.ExecuteCmd[*redis.StringCmd](ctx, StringCmd, GET, args)
//	if cmd.Err() != nil {
//		// 处理错误
//	}
//	val, _ := cmd.Result()
func ExecuteCmd[T redis.Cmder](rdm *RedisClient, ctx context.Context, cmd RdCmd, cmdName Command, args map[string]any, includeArgs ...any) T {
	var zero T
	cmdList, key, subCmd := Build(ctx, cmd, cmdName, args, includeArgs...)

	// 根据泛型类型 T 创建对应的 redis.Cmder
	var cmder redis.Cmder
	switch any(zero).(type) {
	case *redis.StringCmd:
		cmder = redis.NewStringCmd(ctx, cmdList...)
	case *redis.IntCmd:
		cmder = redis.NewIntCmd(ctx, cmdList...)
	case *redis.SliceCmd:
		cmder = redis.NewSliceCmd(ctx, cmdList...)
	case *redis.FloatCmd:
		cmder = redis.NewFloatCmd(ctx, cmdList...)
	case *redis.BoolCmd:
		cmder = redis.NewBoolCmd(ctx, cmdList...)
	case *redis.MapStringIntCmd:
		cmder = redis.NewMapStringIntCmd(ctx, cmdList...)
	case *redis.MapStringStringCmd:
		cmder = redis.NewMapStringStringCmd(ctx, cmdList...)
	case *redis.StringSliceCmd:
		cmder = redis.NewStringSliceCmd(ctx, cmdList...)
	case *redis.IntSliceCmd:
		cmder = redis.NewIntSliceCmd(ctx, cmdList...)
	case *redis.FloatSliceCmd:
		cmder = redis.NewFloatSliceCmd(ctx, cmdList...)
	case *redis.BoolSliceCmd:
		cmder = redis.NewBoolSliceCmd(ctx, cmdList...)
	case *redis.KeyValueSliceCmd:
		cmder = redis.NewKeyValueSliceCmd(ctx, cmdList...)
	case *redis.MapStringInterfaceCmd:
		cmder = redis.NewMapStringInterfaceCmd(ctx, cmdList...)
	case *redis.MapStringStringSliceCmd:
		cmder = redis.NewMapStringStringSliceCmd(ctx, cmdList...)
	case *redis.MapStringInterfaceSliceCmd:
		cmder = redis.NewMapStringInterfaceSliceCmd(ctx, cmdList...)
	case *redis.MapStringSliceInterfaceCmd:
		cmder = redis.NewMapStringSliceInterfaceCmd(ctx, cmdList...)
	case *redis.MapMapStringInterfaceCmd:
		cmder = redis.NewMapMapStringInterfaceCmd(ctx, cmdList...)
	case *redis.ZSliceCmd:
		cmder = redis.NewZSliceCmd(ctx, cmdList...)
	case *redis.ZSliceWithKeyCmd:
		cmder = redis.NewZSliceWithKeyCmd(ctx, cmdList...)
	case *redis.ZWithKeyCmd:
		cmder = redis.NewZWithKeyCmd(ctx, cmdList...)
	default:
		cmder = redis.NewCmd(ctx, cmdList...)
	}

	processErr := rdm.Client.Process(ctx, cmder)
	cmdErr := cmder.Err()
	if processErr != nil {
		cmdErr = processErr
	}
	if !subCmd.ReturnNilError && errors.Is(cmdErr, redis.Nil) {
		cmdErr = nil
	}
	cmder.SetErr(cmdErr)

	// 设置过期时间
	if subCmd.Exp != nil {
		exp := subCmd.Exp()
		expireCmd := rdm.Client.Expire(ctx, key, exp)
		if expireCmd.Err() != nil {
			// 记录错误但不影响主命令
		}
	}

	// 类型断言，确保返回的是期望的类型
	result, ok := cmder.(T)
	if !ok {
		// 如果类型不匹配，返回零值
		// 这种情况理论上不应该发生，因为我们在 switch 中已经创建了正确的类型
		return zero
	}

	return result
}

// ========== CommandBuilder 的链式调用方法 ==========

// String 执行命令并返回 *redis.StringCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) String() *redis.StringCmd {
	// 如果已经执行过，直接返回缓存的 cmder
	if cb.cmder != nil {
		if strCmd, ok := cb.cmder.(*redis.StringCmd); ok {
			return strCmd
		}
	}

	// 如果在 Pipeline 中，使用 Pipeline 模式
	if cb.pipeliner != nil {
		cmdList, key, subCmd := Build(cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cmder := redis.NewStringCmd(cb.ctx, cmdList...)
		_ = cb.pipeliner.Process(cb.ctx, cmder)
		if subCmd.Exp != nil {
			exp := subCmd.Exp()
			cb.pipeliner.Expire(cb.ctx, key, exp)
		}
		cb.cmder = cmder
		return cmder
	}

	return ExecuteCmd[*redis.StringCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// executeCmdInPipeline 在 Pipeline 中执行命令的通用方法（辅助函数）
// 根据期望的返回类型创建对应的 redis.Cmder
// 错误通过返回的 Cmder 的 Err() 方法获取（在 Pipeline Exec() 后）
func executeCmdInPipeline[T redis.Cmder](pipeliner redis.Pipeliner, ctx context.Context, cmd RdCmd, cmdName Command, args map[string]any, includeArgs ...any) T {
	var zero T
	cmdList, key, subCmd := Build(ctx, cmd, cmdName, args, includeArgs...)

	// 根据泛型类型 T 创建对应的 redis.Cmder
	var cmder redis.Cmder
	switch any(zero).(type) {
	case *redis.StringCmd:
		cmder = redis.NewStringCmd(ctx, cmdList...)
	case *redis.IntCmd:
		cmder = redis.NewIntCmd(ctx, cmdList...)
	case *redis.SliceCmd:
		cmder = redis.NewSliceCmd(ctx, cmdList...)
	case *redis.FloatCmd:
		cmder = redis.NewFloatCmd(ctx, cmdList...)
	case *redis.BoolCmd:
		cmder = redis.NewBoolCmd(ctx, cmdList...)
	case *redis.MapStringIntCmd:
		cmder = redis.NewMapStringIntCmd(ctx, cmdList...)
	case *redis.MapStringStringCmd:
		cmder = redis.NewMapStringStringCmd(ctx, cmdList...)
	case *redis.StringSliceCmd:
		cmder = redis.NewStringSliceCmd(ctx, cmdList...)
	case *redis.IntSliceCmd:
		cmder = redis.NewIntSliceCmd(ctx, cmdList...)
	case *redis.FloatSliceCmd:
		cmder = redis.NewFloatSliceCmd(ctx, cmdList...)
	case *redis.BoolSliceCmd:
		cmder = redis.NewBoolSliceCmd(ctx, cmdList...)
	case *redis.KeyValueSliceCmd:
		cmder = redis.NewKeyValueSliceCmd(ctx, cmdList...)
	case *redis.MapStringInterfaceCmd:
		cmder = redis.NewMapStringInterfaceCmd(ctx, cmdList...)
	case *redis.MapStringStringSliceCmd:
		cmder = redis.NewMapStringStringSliceCmd(ctx, cmdList...)
	case *redis.MapStringInterfaceSliceCmd:
		cmder = redis.NewMapStringInterfaceSliceCmd(ctx, cmdList...)
	case *redis.MapStringSliceInterfaceCmd:
		cmder = redis.NewMapStringSliceInterfaceCmd(ctx, cmdList...)
	case *redis.MapMapStringInterfaceCmd:
		cmder = redis.NewMapMapStringInterfaceCmd(ctx, cmdList...)
	case *redis.ZSliceCmd:
		cmder = redis.NewZSliceCmd(ctx, cmdList...)
	case *redis.ZSliceWithKeyCmd:
		cmder = redis.NewZSliceWithKeyCmd(ctx, cmdList...)
	case *redis.ZWithKeyCmd:
		cmder = redis.NewZWithKeyCmd(ctx, cmdList...)
	default:
		cmder = redis.NewCmd(ctx, cmdList...)
	}

	_ = pipeliner.Process(ctx, cmder)
	if subCmd.Exp != nil {
		exp := subCmd.Exp()
		pipeliner.Expire(ctx, key, exp)
	}

	result, ok := cmder.(T)
	if !ok {
		// 如果类型不匹配，返回零值
		// 这种情况理论上不应该发生，因为我们在 switch 中已经创建了正确的类型
		return zero
	}
	return result
}

// Int 执行命令并返回 *redis.IntCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) Int() *redis.IntCmd {
	if cb.cmder != nil {
		if intCmd, ok := cb.cmder.(*redis.IntCmd); ok {
			return intCmd
		}
	}
	if cb.pipeliner != nil {
		intCmd := executeCmdInPipeline[*redis.IntCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = intCmd
		return intCmd
	}
	return ExecuteCmd[*redis.IntCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// Slice 执行命令并返回 *redis.SliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) Slice() *redis.SliceCmd {
	if cb.cmder != nil {
		if sliceCmd, ok := cb.cmder.(*redis.SliceCmd); ok {
			return sliceCmd
		}
	}
	if cb.pipeliner != nil {
		sliceCmd := executeCmdInPipeline[*redis.SliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = sliceCmd
		return sliceCmd
	}
	return ExecuteCmd[*redis.SliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// Float 执行命令并返回 *redis.FloatCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) Float() *redis.FloatCmd {
	if cb.cmder != nil {
		if floatCmd, ok := cb.cmder.(*redis.FloatCmd); ok {
			return floatCmd
		}
	}
	if cb.pipeliner != nil {
		floatCmd := executeCmdInPipeline[*redis.FloatCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = floatCmd
		return floatCmd
	}
	return ExecuteCmd[*redis.FloatCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// Bool 执行命令并返回 *redis.BoolCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) Bool() *redis.BoolCmd {
	if cb.cmder != nil {
		if boolCmd, ok := cb.cmder.(*redis.BoolCmd); ok {
			return boolCmd
		}
	}
	if cb.pipeliner != nil {
		boolCmd := executeCmdInPipeline[*redis.BoolCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = boolCmd
		return boolCmd
	}
	return ExecuteCmd[*redis.BoolCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapStringInt 执行命令并返回 *redis.MapStringIntCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapStringInt() *redis.MapStringIntCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapStringIntCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapStringIntCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapStringIntCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapStringString 执行命令并返回 *redis.MapStringStringCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapStringString() *redis.MapStringStringCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapStringStringCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapStringStringCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapStringStringCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// StringSlice 执行命令并返回 *redis.StringSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) StringSlice() *redis.StringSliceCmd {
	if cb.cmder != nil {
		if strSliceCmd, ok := cb.cmder.(*redis.StringSliceCmd); ok {
			return strSliceCmd
		}
	}
	if cb.pipeliner != nil {
		strSliceCmd := executeCmdInPipeline[*redis.StringSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = strSliceCmd
		return strSliceCmd
	}
	return ExecuteCmd[*redis.StringSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// IntSlice 执行命令并返回 *redis.IntSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) IntSlice() *redis.IntSliceCmd {
	if cb.cmder != nil {
		if intSliceCmd, ok := cb.cmder.(*redis.IntSliceCmd); ok {
			return intSliceCmd
		}
	}
	if cb.pipeliner != nil {
		intSliceCmd := executeCmdInPipeline[*redis.IntSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = intSliceCmd
		return intSliceCmd
	}
	return ExecuteCmd[*redis.IntSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// FloatSlice 执行命令并返回 *redis.FloatSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) FloatSlice() *redis.FloatSliceCmd {
	if cb.cmder != nil {
		if floatSliceCmd, ok := cb.cmder.(*redis.FloatSliceCmd); ok {
			return floatSliceCmd
		}
	}
	if cb.pipeliner != nil {
		floatSliceCmd := executeCmdInPipeline[*redis.FloatSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = floatSliceCmd
		return floatSliceCmd
	}
	return ExecuteCmd[*redis.FloatSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// BoolSlice 执行命令并返回 *redis.BoolSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) BoolSlice() *redis.BoolSliceCmd {
	if cb.cmder != nil {
		if boolSliceCmd, ok := cb.cmder.(*redis.BoolSliceCmd); ok {
			return boolSliceCmd
		}
	}
	if cb.pipeliner != nil {
		boolSliceCmd := executeCmdInPipeline[*redis.BoolSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = boolSliceCmd
		return boolSliceCmd
	}
	return ExecuteCmd[*redis.BoolSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// KeyValueSlice 执行命令并返回 *redis.KeyValueSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) KeyValueSlice() *redis.KeyValueSliceCmd {
	if cb.cmder != nil {
		if kvSliceCmd, ok := cb.cmder.(*redis.KeyValueSliceCmd); ok {
			return kvSliceCmd
		}
	}
	if cb.pipeliner != nil {
		kvSliceCmd := executeCmdInPipeline[*redis.KeyValueSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = kvSliceCmd
		return kvSliceCmd
	}
	return ExecuteCmd[*redis.KeyValueSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapStringInterface 执行命令并返回 *redis.MapStringInterfaceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapStringInterface() *redis.MapStringInterfaceCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapStringInterfaceCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapStringInterfaceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapStringInterfaceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapStringStringSlice 执行命令并返回 *redis.MapStringStringSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapStringStringSlice() *redis.MapStringStringSliceCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapStringStringSliceCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapStringStringSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapStringStringSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapStringInterfaceSlice 执行命令并返回 *redis.MapStringInterfaceSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapStringInterfaceSlice() *redis.MapStringInterfaceSliceCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapStringInterfaceSliceCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapStringInterfaceSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapStringInterfaceSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapStringSliceInterface 执行命令并返回 *redis.MapStringSliceInterfaceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapStringSliceInterface() *redis.MapStringSliceInterfaceCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapStringSliceInterfaceCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapStringSliceInterfaceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapStringSliceInterfaceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// MapMapStringInterface 执行命令并返回 *redis.MapMapStringInterfaceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) MapMapStringInterface() *redis.MapMapStringInterfaceCmd {
	if cb.cmder != nil {
		if mapCmd, ok := cb.cmder.(*redis.MapMapStringInterfaceCmd); ok {
			return mapCmd
		}
	}
	if cb.pipeliner != nil {
		mapCmd := executeCmdInPipeline[*redis.MapMapStringInterfaceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = mapCmd
		return mapCmd
	}
	return ExecuteCmd[*redis.MapMapStringInterfaceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// ZSlice 执行命令并返回 *redis.ZSliceCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) ZSlice() *redis.ZSliceCmd {
	if cb.cmder != nil {
		if zSliceCmd, ok := cb.cmder.(*redis.ZSliceCmd); ok {
			return zSliceCmd
		}
	}
	if cb.pipeliner != nil {
		zSliceCmd := executeCmdInPipeline[*redis.ZSliceCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = zSliceCmd
		return zSliceCmd
	}
	return ExecuteCmd[*redis.ZSliceCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// ZSliceWithKey 执行命令并返回 *redis.ZSliceWithKeyCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) ZSliceWithKey() *redis.ZSliceWithKeyCmd {
	if cb.cmder != nil {
		if zSliceCmd, ok := cb.cmder.(*redis.ZSliceWithKeyCmd); ok {
			return zSliceCmd
		}
	}
	if cb.pipeliner != nil {
		zSliceCmd := executeCmdInPipeline[*redis.ZSliceWithKeyCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = zSliceCmd
		return zSliceCmd
	}
	return ExecuteCmd[*redis.ZSliceWithKeyCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}

// ZWithKey 执行命令并返回 *redis.ZWithKeyCmd
// 如果在 Pipeline 中，命令会被添加到 Pipeline，结果需要在 Exec() 后获取
// 错误通过返回的 Cmder 的 Err() 方法获取
func (cb *CommandBuilder) ZWithKey() *redis.ZWithKeyCmd {
	if cb.cmder != nil {
		if zCmd, ok := cb.cmder.(*redis.ZWithKeyCmd); ok {
			return zCmd
		}
	}
	if cb.pipeliner != nil {
		zCmd := executeCmdInPipeline[*redis.ZWithKeyCmd](cb.pipeliner, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
		cb.cmder = zCmd
		return zCmd
	}
	return ExecuteCmd[*redis.ZWithKeyCmd](cb.client, cb.ctx, cb.cmd, cb.cmdName, cb.args, cb.includeArgs...)
}
