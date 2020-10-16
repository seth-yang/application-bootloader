package org.dreamwork.app.bootloader;

import java.lang.annotation.*;

/**
 * Created by seth.yang on 2020/1/7
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface ApplicationEntrance {
}