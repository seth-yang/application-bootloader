package org.dreamwork.app.bootloader;

import java.lang.annotation.*;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface IBootable {
    String argumentDef () default "";
}