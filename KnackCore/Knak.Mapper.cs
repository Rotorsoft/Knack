using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Knak {
    public static class Mapper {
		public static TargetT To<SourceT, TargetT>(this SourceT source, TargetT target) => PropertyMapper<SourceT, TargetT>.Map(source, target);
		public static TargetT From<SourceT, TargetT>(this TargetT target, SourceT source) => PropertyMapper<SourceT, TargetT>.Map(source, target);
		public static TargetT[] To<SourceT, TargetT>(this SourceT[] source) where SourceT : class where TargetT : class, new() {
			TargetT[] target = null;
			if (source != null) {
				target = new TargetT[source.Length];
				for (int i = 0; i < source.Length; i++)
					if (source[i] != null) target[i] = new TargetT().From(source[i]);
			}
			return target;
		}
        public static TargetT[] Copy<SourceT, TargetT>(this SourceT[] source) {
            TargetT[] target = null;
            if (source != null) {
                target = new TargetT[source.Length];
                source.CopyTo(target, 0);
            }
            return target;
        }

        public class Of<SourceT, TargetT> {
            private Of() { }
            public Of<SourceT, TargetT> Bind<T>(Expression<Func<SourceT, T>> source, Expression<Func<TargetT, T>> target) { PropertyMapper<SourceT, TargetT>.Bind(source, target); return this; }
            public Of<SourceT, TargetT> Ignore<T>(Expression<Func<TargetT, T>> target) { PropertyMapper<SourceT, TargetT>.Ignore(target); return this; }
            public Of<SourceT, TargetT> Reset() { PropertyMapper<SourceT, TargetT>.Reset(); return this; }
            public static Of<SourceT, TargetT> Binder { get { return new Of<SourceT, TargetT>(); } }
            public static IEnumerable<IMapping> Mappings { get { return Flatten(PropertyMapper<SourceT, TargetT>.GetMappings(), string.Empty); } }
			public static LambdaExpression Expression { get { return PropertyMapper<SourceT, TargetT>.GetLambdaExpression(); } }
        }

        class PropertyMapping : IMapping {
            public string Source { get; private set; }
            public string Target { get { return TargetProperty.Name; } }
            public Type SourceType { get; private set; }
            public Type TargetType { get { return TargetProperty.PropertyType; } }

            public PropertyInfo TargetProperty { get; private set; }
            public IList<IMapping> Next { get; private set; }
            public LambdaExpression Binding { get; private set; }

            public PropertyMapping(string source, Type sourceType, IList<IMapping> next) {
                Source = source;
                SourceType = sourceType;
                Next = next;
            }

            public PropertyMapping(string source, Type sourceType, PropertyInfo targetProperty) {
                Source = source;
                SourceType = sourceType;
                TargetProperty = targetProperty;
            }

            public PropertyMapping(PropertyInfo targetProperty, LambdaExpression binding) {
                Source = binding.ToString();
                SourceType = targetProperty.PropertyType;
                TargetProperty = targetProperty;
                Binding = binding;
            }
        }

        static class PropertyMapper<SourceT, TargetT> {
            delegate void MapperCallback(SourceT source, TargetT target);
            static MapperCallback mapper;
            static IEnumerable<IMapping> mappings;
            static IDictionary<PropertyInfo, LambdaExpression> bindings = new ConcurrentDictionary<PropertyInfo, LambdaExpression>(); // custom bindings indexed by target

            public static IEnumerable<IMapping> GetMappings() {
                lock (bindings) {
                    if (mappings == null) mappings = Mapper.GetMappings(typeof(SourceT), typeof(TargetT), bindings);
                    return mappings;
                }
            }

            private static Expression<MapperCallback> GetExpression() { return Mapper.GetMapperExpression<MapperCallback>(typeof(SourceT), typeof(TargetT), GetMappings()); }
            public static LambdaExpression GetLambdaExpression() { return GetExpression(); }
 
            public static TargetT Map(SourceT source, TargetT target) {
                lock (bindings) {
                    if (mapper == null) mapper = GetExpression().Compile();
					if(source != null && target != null) mapper(source, target);
                }
                return target;
            }

            public static void Bind<T>(Expression<Func<SourceT, T>> source, Expression<Func<TargetT, T>> target) {
                var targetProperty = target.ToPropertyInfo();
                if (!targetProperty.CanWrite) throw new ArgumentException("Target property not writeable.", "target");
                bindings[targetProperty] = source;
            }

            public static void Ignore<T>(Expression<Func<TargetT, T>> target) {
                var targetProperty = target.ToPropertyInfo();
                bindings[targetProperty] = null; // will target property ignore when lambda is null
            }

            public static void Reset() {
                lock (bindings) {
                    mapper = null;
                    mappings = null;
                    bindings.Clear();
                }
            }
        }

		const int MAX_LEVEL = 5; // max level when flattening source type

        static Expression<TDelegate> GetMapperExpression<TDelegate>(Type sourceType, Type targetType, IEnumerable<IMapping> mappings) {
            var source = Expression.Parameter(sourceType, "source");
            var target = Expression.Parameter(targetType, "target");
			Expression block = Expression.Empty();
            if (mappings.Count() > 0) block = Expression.Block(GetMappingExpressions(mappings, source, target, string.Empty, source));
            return Expression.Lambda<TDelegate>(block, "Mapper", new[] { source, target });
        }

        static IEnumerable<IMapping> Flatten(IEnumerable<IMapping> mappings, string source) {
            foreach (PropertyMapping m in mappings)
                if (m.Next != null)
                    foreach (var f in Flatten(m.Next, string.Concat(source, ".", m.Source))) yield return f;
                else yield return new PropertyMapping(string.Concat(source, ".", m.Source).Substring(1), m.SourceType, m.TargetProperty);
        }

        static IEnumerable<IMapping> GetMappings(Type sourceType, Type targetType, IDictionary<PropertyInfo, LambdaExpression> bindings) {
            var mappings = bindings.Where(e => e.Value != null).Select(e => new PropertyMapping(e.Key, e.Value)).ToList<IMapping>(); // add bindings with lambdas
            var targets = targetType.GetMappableTargetProperties(false).Where(e => !bindings.ContainsKey(e)).ToDictionary(p => p.Name); // skip bindings
            mappings.AddRange(GetMappings(targets, sourceType, string.Empty, 0));
            return mappings;
        }

        static IEnumerable<IMapping> GetMappings(IDictionary<string, PropertyInfo> targets, Type type, string source, int level) {
            var toTraverse = new List<PropertyInfo>();
            foreach (var sourceProperty in type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy)) {
                if (targets.Count == 0) break; // stop when all target properties have been mapped
                if (!sourceProperty.CanRead) continue; // only readable properties
                if (sourceProperty.GetCustomAttributes(typeof(IgnoreAttribute), false).Length > 0) continue; // skip source properties marked with IGNORE attribute 
                if (sourceProperty.GetCustomAttributes(typeof(InputAttribute), false).Length > 0) continue; // skip source properties marked with INPUT attribute

                PropertyInfo targetProperty = null;
                if (targets.TryGetValue(sourceProperty.Name, out targetProperty)) { // match property names
                    if (sourceProperty.PropertyType.IsMappableTo(targetProperty.PropertyType, 0)) {
                        yield return new PropertyMapping(sourceProperty.Name, sourceProperty.PropertyType, targetProperty);
                        targets.Remove(targetProperty.Name); // remove from targets
                        continue;
                    }
                }
                if (sourceProperty.PropertyType.IsTraversable() && level < MAX_LEVEL) toTraverse.Add(sourceProperty);
            }
            foreach (var sourceProperty in toTraverse) { // traverse source type hierarchy
                if (targets.Count == 0) break;
                var sourcePath = string.Concat(string.IsNullOrEmpty(source) ? string.Empty : string.Concat(source, "."), sourceProperty.Name);
                var next = GetMappings(targets, sourceProperty.PropertyType, sourcePath, level + 1).ToList();
                if (next.Count() > 0) yield return new PropertyMapping(sourceProperty.Name, sourceProperty.PropertyType, next);
            }
        }

        static bool IsMappableTo(this Type source, Type target, int level) {
            if (source.IsDirect() && source.CanMapTo(target)) return true; // direct-compatible property types
            if (source.IsClass && source == target && source.GetConstructor(Type.EmptyTypes) != null) return true; // classes with default constructor
            if (level == 0 && source.IsArray && target.IsArray) return source.GetElementType().IsMappableTo(target.GetElementType(), level + 1); // arrays of mappable types
            return false;
        }

        static IEnumerable<Expression> GetMappingExpressions(IEnumerable<IMapping> mappings, Expression source, Expression target, string sourcepath, Expression sourceobj) {
            foreach (PropertyMapping mapping in mappings) {
                if (mapping.Binding != null)
					yield return Expression.Assign(Expression.Property(target, mapping.Target), Expression.Invoke(mapping.Binding, source));
                else {
                    var sourcep = Expression.Property(source, mapping.Source);
                    if (mapping.Next != null) {
						var path = string.Concat((string.IsNullOrEmpty(sourcepath) ? string.Empty : string.Concat(sourcepath, ".")), mapping.Source);
						var sobj = mapping.SourceType.IsValueType ? sourceobj : sourcep;
                        Expression block = Expression.Block(GetMappingExpressions(mapping.Next, sourcep, target, path, sobj));
						yield return mapping.SourceType.IsValueType ? block : Expression.IfThen(Expression.NotEqual(sourcep, Expression.Constant(null)), block);
                    }
                    else {
						var targetp = Expression.Property(target, mapping.Target);
						if (mapping.TargetType.IsDirect()) {
							bool nullablecheck = mapping.SourceType.IsNullable() && !mapping.TargetType.IsNullable();
							Expression sourcev = nullablecheck ? Expression.Property(sourcep, "Value") : sourcep;
							if (mapping.SourceType != mapping.TargetType) sourcev = Expression.Convert(sourcev, mapping.TargetType);
							Expression assign = Expression.Assign(targetp, sourcev);
							yield return nullablecheck ? Expression.IfThen(Expression.Property(sourcep, "HasValue"), assign) : assign; 
						}
                        else if (mapping.TargetType.IsArray) {
                            var sourceElementType = mapping.SourceType.GetElementType();
                            var targetElementType = mapping.TargetType.GetElementType();
                            if (sourceElementType.IsClass) {
                                var to = typeof(Mapper).GetMethods().Where(m => m.Name == "To" && m.ReturnType.IsArray).First();
                                var maparray = to.MakeGenericMethod(sourceElementType, targetElementType);
								yield return Expression.Assign(targetp, Expression.Call(maparray, sourcep));
                            }
                            else {
                                var copy = typeof(Mapper).GetMethod("Copy").MakeGenericMethod(sourceElementType, targetElementType);
								yield return Expression.Assign(targetp, Expression.Call(copy, sourcep));
                            }
                        }
                        else if (mapping.TargetType.IsClass) {
                            var to = typeof(Mapper).GetMethods().Where(m => m.Name == "To" && !m.ReturnType.IsArray).First();
							var mapto = to.MakeGenericMethod(mapping.SourceType, mapping.TargetType);
							yield return Expression.IfThen(Expression.And(Expression.NotEqual(sourcep, Expression.Constant(null)), Expression.Equal(targetp, Expression.Constant(null))), Expression.Assign(targetp, Expression.New(mapping.TargetType)));
							yield return Expression.Call(mapto, sourcep, targetp);
						}
                    }
                }
            }
        }

		static Dictionary<Type, Type[]> castables = new Dictionary<Type, Type[]>() {
			{ typeof(decimal),	new [] { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(char) } },
			{ typeof(double),	new [] { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(char), typeof(float) } },
			{ typeof(float),	new [] { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(char), typeof(float) } },
			{ typeof(ulong),	new [] { typeof(byte), typeof(ushort), typeof(uint), typeof(char) } },
			{ typeof(long),		new [] { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(char) } },
			{ typeof(uint),		new [] { typeof(byte), typeof(ushort), typeof(char) } },
			{ typeof(int),		new [] { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(char) } },
			{ typeof(ushort),	new [] { typeof(byte), typeof(char) } },
			{ typeof(short),	new [] { typeof(byte) } }
		};

		public static bool CanMapTo(this Type from, Type to) {
			if (to.IsNullable()) to = to.GetGenericArguments()[0];
			if (from.IsNullable()) from = from.GetGenericArguments()[0];
			if (to.IsAssignableFrom(from)) return true;
			if (castables.ContainsKey(to) && castables[to].Contains(from)) return true;
			return from.GetMethods(BindingFlags.Public | BindingFlags.Static).Any(m => m.ReturnType == to && (m.Name == "op_Implicit" || m.Name == "op_Explicit"));
		}

		public static bool IsNullable(this Type type) { return type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>)); }
		public static bool IsDirect(this Type type) { return type.IsPrimitive || type.IsValueType || type == typeof(string); }
		public static bool IsEnumerable(this Type type) { return !type.IsDirect() && (type.IsArray || type.GetInterface(typeof(ICollection).FullName) != null || type.GetInterface(typeof(IEnumerable).FullName) != null || type.GetInterface(typeof(IEnumerable<>).FullName) != null); }
        public static bool IsTraversable(this Type type) { return !type.IsDirect() && type.IsClass && !type.IsEnumerable(); } // don't traverse enumerables

		public static PropertyInfo ToPropertyInfo<T, TResult>(this Expression<Func<T, TResult>> expression) {
			MemberExpression memberExpression = null;
			if (expression.Body.NodeType == ExpressionType.Convert) memberExpression = ((UnaryExpression)expression.Body).Operand as MemberExpression;
			else if (expression.Body.NodeType == ExpressionType.MemberAccess) memberExpression = expression.Body as MemberExpression;
			if (memberExpression != null) {
				PropertyInfo pinfo = memberExpression.Member as PropertyInfo;
				if (pinfo != null) return pinfo;
			}
			throw new ArgumentException("Expression not a property.", "expression");
		}

		public static IEnumerable<PropertyInfo> GetMappableProperties(this Type type, bool directOnly) {
            foreach (var p in type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy)) {
				if (!(p.PropertyType.IsDirect() || !directOnly && (p.PropertyType.IsClass || p.PropertyType.IsArray))) continue;
				if (p.GetCustomAttributes(typeof(IgnoreAttribute), false).Length > 0) continue; // skip properties with IGNORE attribute
				yield return p;
			}
		}

        public static IEnumerable<PropertyInfo> GetMappableTargetProperties(this Type targetType, bool directOnly) {
            foreach (var p in targetType.GetMappableProperties(directOnly)) {
                if (!p.CanWrite) continue; // only writeable properties 
                if (p.GetCustomAttributes(typeof(OutputAttribute), false).Length > 0) continue; // skip target properties with OUTPUT attribute
				yield return p;
			}
		}
	}

	[AttributeUsage(AttributeTargets.Property)]	public class InputAttribute  : Attribute { } // properties to be mapped ONLY when object is the target 
	[AttributeUsage(AttributeTargets.Property)]	public class OutputAttribute : Attribute { } // properties to be mapped ONLY when object is the source 
	[AttributeUsage(AttributeTargets.Property)]	public class IgnoreAttribute : Attribute { } // properties to IGNORE

	public interface IMapping {
		string Source { get; } Type SourceType { get; }
		string Target { get; } Type TargetType { get; }
	}
}
